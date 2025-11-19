/*
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.v2;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.kafkarest.ConsumerInstanceId;
import io.confluent.kafkarest.ConsumerRecordAndSize;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import io.confluent.kafkarest.converters.ProtobufConverter;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.v2.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.v2.ConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.v2.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.v2.ConsumerCommittedResponse;
import io.confluent.kafkarest.entities.v2.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSeekRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionResponse;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestServerErrorException;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages consumer instances using non-blocking async patterns with CompletableFuture.
 * This class provides an alternative to KafkaConsumerManager that uses a ScheduledExecutorService
 * for polling instead of a traditional thread pool, reducing thread usage and improving scalability.
 */
public class AsyncConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(AsyncConsumerManager.class);

  private final KafkaRestConfig config;
  private final Clock clock;
  private final ScheduledExecutorService scheduler;
  private final int pollIntervalMs;
  private final int maxPendingPolls;
  private final AtomicInteger totalPendingPolls = new AtomicInteger(0);
  private final AtomicBoolean isRunning = new AtomicBoolean(true);

  @GuardedBy("this")
  private final Map<ConsumerInstanceId, AsyncConsumerState<?, ?, ?, ?>> consumers = new HashMap<>();

  private KafkaConsumerManager.KafkaConsumerFactory consumerFactory;
  private final ExpirationThread expirationThread;

  public AsyncConsumerManager(final KafkaRestConfig config) {
    this(config, Clock.systemUTC());
  }

  // Package-private constructor for testing with custom clock
  AsyncConsumerManager(final KafkaRestConfig config, Clock clock) {
    this.config = config;
    this.clock = clock;
    this.pollIntervalMs = config.getAsyncConsumerPollIntervalMs();
    this.maxPendingPolls = config.getAsyncConsumerMaxPendingPolls();

    int schedulerThreads = config.getAsyncConsumerSchedulerThreads();
    this.scheduler = Executors.newScheduledThreadPool(
        schedulerThreads,
        new ThreadFactoryBuilder()
            .setNameFormat("consumer-poll-%d")
            .setDaemon(true)
            .build());

    this.consumerFactory = null;
    this.expirationThread = new ExpirationThread();
    this.expirationThread.start();
  }

  // Package-private constructor for testing with custom consumer factory
  AsyncConsumerManager(KafkaRestConfig config, KafkaConsumerManager.KafkaConsumerFactory consumerFactory) {
    this(config);
    this.consumerFactory = consumerFactory;
  }

  /**
   * Creates a new consumer instance and returns its unique ID.
   *
   * @param group Name of the consumer group to join
   * @param instanceConfig configuration parameters for the consumer
   * @return Unique consumer instance ID
   */
  public String createConsumer(String group, ConsumerInstanceConfig instanceConfig) {
    String name = getConsumerInstanceName(instanceConfig);
    ConsumerInstanceId cid = new ConsumerInstanceId(group, name);

    synchronized (this) {
      if (consumers.containsKey(cid)) {
        throw Errors.consumerAlreadyExistsException();
      } else {
        // Placeholder to reserve this ID
        consumers.put(cid, null);
      }
    }

    boolean succeeded = false;
    try {
      log.debug("Creating async consumer {} in group {}", name, group);

      Properties props = getConsumerInstanceProperties(group, instanceConfig);

      Consumer consumer;
      try {
        if (consumerFactory == null) {
          consumer = new KafkaConsumer(props);
        } else {
          consumer = consumerFactory.createConsumer(props);
        }
      } catch (ConfigException e) {
        throw Errors.invalidConsumerConfigException(e.getMessage());
      }

      KafkaConsumerState state = createConsumerState(instanceConfig, cid, consumer);
      Duration instanceTimeout = Duration.ofMillis(
          config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG));
      AsyncConsumerState asyncState = new AsyncConsumerState(state, cid, instanceTimeout);

      synchronized (this) {
        consumers.put(cid, asyncState);
      }
      succeeded = true;
      return name;
    } finally {
      if (!succeeded) {
        synchronized (this) {
          consumers.remove(cid);
        }
      }
    }
  }

  /**
   * Reads records from a consumer instance asynchronously.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param consumerStateType the expected consumer state type
   * @param timeout the maximum time to wait for records
   * @param maxBytes the maximum bytes to return
   * @return a CompletableFuture that completes with the list of records
   */
  public <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>
      CompletableFuture<List<ConsumerRecord<ClientKeyT, ClientValueT>>> readRecords(
          String group,
          String instance,
          Class<? extends KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
              consumerStateType,
          Duration timeout,
          long maxBytes) {

    CompletableFuture<List<ConsumerRecord<ClientKeyT, ClientValueT>>> future =
        new CompletableFuture<>();

    AsyncConsumerState<?, ?, ?, ?> asyncState;
    try {
      asyncState = getConsumerInstance(group, instance);
    } catch (RestNotFoundException e) {
      future.completeExceptionally(e);
      return future;
    }

    if (!consumerStateType.isInstance(asyncState.getConsumerState())) {
      future.completeExceptionally(Errors.consumerFormatMismatch());
      return future;
    }

    // Check backpressure
    if (totalPendingPolls.get() >= maxPendingPolls) {
      future.completeExceptionally(
          new RestServerErrorException(
              "Too many pending polls. Please retry later.",
              Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
      return future;
    }

    @SuppressWarnings("unchecked")
    AsyncConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> typedAsyncState =
        (AsyncConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>) asyncState;

    // Track pending polls
    totalPendingPolls.incrementAndGet();
    typedAsyncState.incrementPendingPolls();

    // Schedule polling
    schedulePolling(typedAsyncState, future, timeout, maxBytes, clock.instant(), 0L);

    return future;
  }

  private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void schedulePolling(
      AsyncConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> asyncState,
      CompletableFuture<List<ConsumerRecord<ClientKeyT, ClientValueT>>> future,
      Duration timeout,
      long maxBytes,
      Instant startTime,
      long bytesConsumed) {

    scheduler.schedule(() -> {
      try {
        if (!isRunning.get()) {
          future.completeExceptionally(
              new RestServerErrorException(
                  "Consumer manager is shutting down",
                  Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
          decrementPendingPolls(asyncState);
          return;
        }

        KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> consumerState =
            asyncState.getConsumerState();

        List<ConsumerRecord<ClientKeyT, ClientValueT>> records = new ArrayList<>();
        long currentBytesConsumed = bytesConsumed;

        // Non-blocking poll - check if there are records available
        synchronized (consumerState) {
          while (consumerState.hasNext() && currentBytesConsumed < maxBytes) {
            ConsumerRecordAndSize<ClientKeyT, ClientValueT> recordAndSize =
                consumerState.createConsumerRecord(consumerState.next());
            records.add(recordAndSize.getRecord());
            currentBytesConsumed += recordAndSize.getSize();
          }
        }

        asyncState.updateExpiration();

        boolean hasRecords = !records.isEmpty();
        boolean timeoutExpired = clock.instant().isAfter(startTime.plus(timeout));
        boolean maxBytesReached = currentBytesConsumed >= maxBytes;

        if (hasRecords || timeoutExpired || maxBytesReached) {
          // Records available or timeout/max bytes reached, complete the future
          future.complete(records);
          decrementPendingPolls(asyncState);
          log.trace("Completed async read for consumer {} with {} records",
              asyncState.getInstanceId(), records.size());
        } else {
          // Reschedule polling
          schedulePolling(asyncState, future, timeout, maxBytes, startTime, currentBytesConsumed);
        }
      } catch (Exception e) {
        log.error("Failed to read records from async consumer {}", asyncState.getInstanceId(), e);
        future.completeExceptionally(e);
        decrementPendingPolls(asyncState);
      }
    }, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  private void decrementPendingPolls(AsyncConsumerState<?, ?, ?, ?> asyncState) {
    totalPendingPolls.decrementAndGet();
    asyncState.decrementPendingPolls();
  }

  /**
   * Commits offsets for a consumer instance asynchronously.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param async whether to use async commit
   * @param offsetCommitRequest the offset commit request
   * @return a CompletableFuture that completes with the committed offsets
   */
  public CompletableFuture<List<TopicPartitionOffset>> commitOffsets(
      String group,
      String instance,
      String async,
      ConsumerOffsetCommitRequest offsetCommitRequest) {

    CompletableFuture<List<TopicPartitionOffset>> future = new CompletableFuture<>();

    AsyncConsumerState<?, ?, ?, ?> asyncState;
    try {
      asyncState = getConsumerInstance(group, instance);
    } catch (RestNotFoundException e) {
      future.completeExceptionally(e);
      return future;
    }

    scheduler.execute(() -> {
      try {
        List<TopicPartitionOffset> offsets =
            asyncState.getConsumerState().commitOffsets(async, offsetCommitRequest);
        asyncState.updateExpiration();
        future.complete(offsets);
      } catch (Exception e) {
        log.error("Failed to commit offsets for async consumer {}", asyncState.getInstanceId(), e);
        future.completeExceptionally(e);
      }
    });

    return future;
  }

  /**
   * Gets committed offsets for a consumer instance.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param request the committed request
   * @return the committed offsets
   */
  public ConsumerCommittedResponse committed(
      String group, String instance, ConsumerCommittedRequest request) {
    log.debug("Committed offsets for consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      return state.getConsumerState().committed(request);
    } else {
      return new ConsumerCommittedResponse(new ArrayList<>());
    }
  }

  /**
   * Deletes a consumer instance.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   */
  public void deleteConsumer(String group, String instance) {
    log.debug("Destroying async consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance, true);
    state.close();
  }

  /**
   * Subscribes a consumer to topics.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param subscription the subscription record
   */
  public void subscribe(String group, String instance, ConsumerSubscriptionRecord subscription) {
    log.debug("Subscribing async consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      state.getConsumerState().subscribe(subscription);
    }
  }

  /**
   * Unsubscribes a consumer from all topics.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   */
  public void unsubscribe(String group, String instance) {
    log.debug("Unsubscribing async consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      state.getConsumerState().unsubscribe();
    }
  }

  /**
   * Gets the current subscription for a consumer.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @return the subscription response
   */
  public ConsumerSubscriptionResponse subscription(String group, String instance) {
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      return new ConsumerSubscriptionResponse(
          new ArrayList<>(state.getConsumerState().subscription()));
    } else {
      return new ConsumerSubscriptionResponse(new ArrayList<>());
    }
  }

  /**
   * Seeks to the beginning of partitions.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param seekToRequest the seek request
   */
  public void seekToBeginning(String group, String instance, ConsumerSeekToRequest seekToRequest) {
    log.debug("Seeking to beginning for async consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      state.getConsumerState().seekToBeginning(seekToRequest);
    }
  }

  /**
   * Seeks to the end of partitions.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param seekToRequest the seek request
   */
  public void seekToEnd(String group, String instance, ConsumerSeekToRequest seekToRequest) {
    log.debug("Seeking to end for async consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      state.getConsumerState().seekToEnd(seekToRequest);
    }
  }

  /**
   * Seeks to a specific offset.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param request the seek request
   */
  public void seek(String group, String instance, ConsumerSeekRequest request) {
    log.debug("Seeking to offset for async consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      state.getConsumerState().seek(request);
    }
  }

  /**
   * Assigns partitions to a consumer.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @param assignmentRequest the assignment request
   */
  public void assign(String group, String instance, ConsumerAssignmentRequest assignmentRequest) {
    log.debug("Assigning partitions to async consumer {} in group {}", instance, group);
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      state.getConsumerState().assign(assignmentRequest);
    }
  }

  /**
   * Gets the current assignment for a consumer.
   *
   * @param group the consumer group
   * @param instance the consumer instance
   * @return the assignment response
   */
  public ConsumerAssignmentResponse assignment(String group, String instance) {
    log.debug("Getting assignment for async consumer {} in group {}", instance, group);
    Vector<io.confluent.kafkarest.entities.v2.TopicPartition> partitions = new Vector<>();
    AsyncConsumerState<?, ?, ?, ?> state = getConsumerInstance(group, instance);
    if (state != null) {
      for (TopicPartition t : state.getConsumerState().assignment()) {
        partitions.add(
            new io.confluent.kafkarest.entities.v2.TopicPartition(t.topic(), t.partition()));
      }
    }
    return new ConsumerAssignmentResponse(partitions);
  }

  /**
   * Shuts down the async consumer manager.
   */
  public void shutdown() {
    log.debug("Shutting down async consumers");
    isRunning.set(false);
    expirationThread.shutdown();

    synchronized (this) {
      for (AsyncConsumerState<?, ?, ?, ?> state : consumers.values()) {
        if (state != null) {
          state.close();
        }
      }
      consumers.clear();
    }

    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private synchronized AsyncConsumerState<?, ?, ?, ?> getConsumerInstance(
      String group, String instance, boolean toRemove) {
    ConsumerInstanceId id = new ConsumerInstanceId(group, instance);
    final AsyncConsumerState<?, ?, ?, ?> state =
        toRemove ? consumers.remove(id) : consumers.get(id);
    if (state == null) {
      throw Errors.consumerInstanceNotFoundException();
    }
    state.updateExpiration();
    return state;
  }

  AsyncConsumerState<?, ?, ?, ?> getConsumerInstance(String group, String instance) {
    return getConsumerInstance(group, instance, false);
  }

  private String getConsumerInstanceName(ConsumerInstanceConfig instanceConfig) {
    if (instanceConfig.getId() != null) {
      return instanceConfig.getId();
    }
    if (instanceConfig.getName() != null) {
      return instanceConfig.getName();
    }
    StringBuilder name = new StringBuilder("rest-consumer-");
    String serverId = this.config.getString(KafkaRestConfig.ID_CONFIG);
    if (!serverId.isEmpty()) {
      name.append(serverId + "-");
    }
    name.append(UUID.randomUUID().toString());
    return name.toString();
  }

  private Properties getConsumerInstanceProperties(
      String group, ConsumerInstanceConfig instanceConfig) {
    Properties props = config.getConsumerProperties();
    props.setProperty("group.id", group);
    if (instanceConfig.getId() != null) {
      props.setProperty("consumer.id", instanceConfig.getId());
    }
    if (instanceConfig.getAutoCommitEnable() != null) {
      props.setProperty("enable.auto.commit", instanceConfig.getAutoCommitEnable());
    }
    if (instanceConfig.getAutoOffsetReset() != null) {
      props.setProperty("auto.offset.reset", instanceConfig.getAutoOffsetReset());
    }
    props.setProperty("request.timeout.ms", "30000");

    switch (instanceConfig.getFormat()) {
      case AVRO:
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        break;
      case JSONSCHEMA:
        props.put(
            "key.deserializer", "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        props.put(
            "value.deserializer",
            "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        break;
      case PROTOBUF:
        props.put(
            "key.deserializer",
            "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
        props.put(
            "value.deserializer",
            "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
        break;
      case JSON:
      case BINARY:
      default:
        props.put(
            "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(
            "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    return props;
  }

  private KafkaConsumerState createConsumerState(
      ConsumerInstanceConfig instanceConfig, ConsumerInstanceId cid, Consumer consumer)
      throws RestServerErrorException {
    switch (instanceConfig.getFormat()) {
      case BINARY:
        return new BinaryKafkaConsumerState(config, instanceConfig, cid, consumer);
      case AVRO:
        return new SchemaKafkaConsumerState(
            config, instanceConfig, cid, consumer, new AvroConverter());
      case JSON:
        return new JsonKafkaConsumerState(config, instanceConfig, cid, consumer);
      case JSONSCHEMA:
        return new SchemaKafkaConsumerState(
            config, instanceConfig, cid, consumer, new JsonSchemaConverter());
      case PROTOBUF:
        return new SchemaKafkaConsumerState(
            config, instanceConfig, cid, consumer, new ProtobufConverter());
      default:
        throw new RestServerErrorException(
            String.format(
                "Invalid embedded format %s for new consumer.", instanceConfig.getFormat()),
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private class ExpirationThread extends Thread {
    AtomicBoolean isRunning = new AtomicBoolean(true);

    public ExpirationThread() {
      super("Async Consumer Expiration Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        while (isRunning.get()) {
          synchronized (AsyncConsumerManager.this) {
            Instant now = clock.instant();
            Iterator<Map.Entry<ConsumerInstanceId, AsyncConsumerState<?, ?, ?, ?>>> itr =
                consumers.entrySet().iterator();
            while (itr.hasNext()) {
              Map.Entry<ConsumerInstanceId, AsyncConsumerState<?, ?, ?, ?>> entry = itr.next();
              AsyncConsumerState<?, ?, ?, ?> state = entry.getValue();
              if (state != null && state.expired(now)) {
                log.debug("Removing the expired async consumer {}", state.getInstanceId());
                itr.remove();
                scheduler.execute(state::close);
              }
            }
          }

          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        // Interrupted by other thread, exit
      }
    }

    public void shutdown() {
      isRunning.set(false);
      this.interrupt();
      try {
        this.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
