/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafkarest.producer;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pool of Kafka producers that can manage per-topic producers with idle cleanup.
 *
 * <p>This implementation creates producers on-demand for topics and caches them for reuse. Idle
 * producers are cleaned up after a configurable timeout to free resources.
 */
public class DefaultProducerPool implements ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(DefaultProducerPool.class);

  private final ConcurrentHashMap<String, ProducerEntry> producers;
  private final Map<String, Object> defaultConfig;
  private final Map<String, Map<String, Object>> topicConfigs;
  private final int maxPoolSize;
  private final AtomicInteger currentSize;
  private final Duration idleTimeout;
  private final Clock clock;
  private final ScheduledExecutorService cleanupExecutor;
  private final ProducerPoolMetrics metrics;

  // Entry to track producer with last access time
  private static class ProducerEntry {
    final Producer<byte[], byte[]> producer;
    volatile long lastAccessTime;

    ProducerEntry(Producer<byte[], byte[]> producer, long accessTime) {
      this.producer = producer;
      this.lastAccessTime = accessTime;
    }

    void updateAccessTime(long time) {
      this.lastAccessTime = time;
    }
  }

  /**
   * Creates a new DefaultProducerPool.
   *
   * @param defaultConfig the default producer configuration
   * @param topicConfigs per-topic configuration overrides
   * @param maxPoolSize the maximum number of producers in the pool
   * @param idleTimeout duration after which idle producers are cleaned up
   * @param metrics metrics collector for the pool
   */
  public DefaultProducerPool(
      Map<String, Object> defaultConfig,
      Map<String, Map<String, Object>> topicConfigs,
      int maxPoolSize,
      Duration idleTimeout,
      ProducerPoolMetrics metrics) {
    this(defaultConfig, topicConfigs, maxPoolSize, idleTimeout, metrics, Clock.systemUTC());
  }

  /**
   * Creates a new DefaultProducerPool with a custom clock (for testing).
   *
   * @param defaultConfig the default producer configuration
   * @param topicConfigs per-topic configuration overrides
   * @param maxPoolSize the maximum number of producers in the pool
   * @param idleTimeout duration after which idle producers are cleaned up
   * @param metrics metrics collector for the pool
   * @param clock the clock to use for time-based operations
   */
  public DefaultProducerPool(
      Map<String, Object> defaultConfig,
      Map<String, Map<String, Object>> topicConfigs,
      int maxPoolSize,
      Duration idleTimeout,
      ProducerPoolMetrics metrics,
      Clock clock) {
    this.producers = new ConcurrentHashMap<>();
    this.defaultConfig = requireNonNull(defaultConfig);
    this.topicConfigs = topicConfigs != null ? topicConfigs : new HashMap<>();
    this.maxPoolSize = maxPoolSize;
    this.currentSize = new AtomicInteger(0);
    this.idleTimeout = requireNonNull(idleTimeout);
    this.clock = requireNonNull(clock);
    this.metrics = metrics;
    this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = new Thread(r, "producer-pool-cleanup");
          t.setDaemon(true);
          return t;
        });

    // Schedule cleanup task
    long cleanupIntervalMs = Math.max(idleTimeout.toMillis() / 2, 30000);
    cleanupExecutor.scheduleAtFixedRate(
        this::cleanupIdleProducers,
        cleanupIntervalMs,
        cleanupIntervalMs,
        TimeUnit.MILLISECONDS);

    log.info(
        "Producer pool initialized with maxPoolSize={}, idleTimeout={}ms",
        maxPoolSize,
        idleTimeout.toMillis());
  }

  @Override
  public Producer<byte[], byte[]> getProducer(String topicName) {
    requireNonNull(topicName, "topicName cannot be null");

    ProducerEntry entry = producers.computeIfAbsent(topicName, this::createProducerEntry);
    entry.updateAccessTime(clock.millis());

    if (metrics != null) {
      metrics.recordPoolHit();
    }

    return entry.producer;
  }

  private ProducerEntry createProducerEntry(String topicName) {
    // Check pool size limit
    int size = currentSize.incrementAndGet();
    if (size > maxPoolSize) {
      currentSize.decrementAndGet();
      log.warn(
          "Producer pool size limit reached ({}). Reusing default producer for topic: {}",
          maxPoolSize,
          topicName);
      if (metrics != null) {
        metrics.recordPoolMiss();
      }
      // Return a new entry that uses the default producer (first topic's producer)
      // This ensures we don't exceed the pool limit
      ProducerEntry defaultEntry = producers.values().iterator().next();
      return new ProducerEntry(defaultEntry.producer, clock.millis());
    }

    // Get topic-specific config or fall back to default
    Map<String, Object> config = getConfigForTopic(topicName);

    try {
      Producer<byte[], byte[]> producer =
          new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());

      log.debug(
          "Created new producer for topic: {}, pool size: {}/{}",
          topicName,
          currentSize.get(),
          maxPoolSize);

      if (metrics != null) {
        metrics.recordPoolSize(currentSize.get());
      }

      return new ProducerEntry(producer, clock.millis());
    } catch (Exception e) {
      currentSize.decrementAndGet();
      log.error("Failed to create producer for topic: {}", topicName, e);
      throw e;
    }
  }

  private Map<String, Object> getConfigForTopic(String topicName) {
    Map<String, Object> topicConfig = topicConfigs.get(topicName);
    if (topicConfig == null || topicConfig.isEmpty()) {
      return defaultConfig;
    }

    // Merge default config with topic-specific overrides
    Map<String, Object> mergedConfig = new HashMap<>(defaultConfig);
    mergedConfig.putAll(topicConfig);
    return mergedConfig;
  }

  private void cleanupIdleProducers() {
    long now = clock.millis();
    long idleTimeoutMs = idleTimeout.toMillis();
    int evicted = 0;

    Iterator<Map.Entry<String, ProducerEntry>> iterator = producers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, ProducerEntry> entry = iterator.next();
      ProducerEntry producerEntry = entry.getValue();

      if (now - producerEntry.lastAccessTime > idleTimeoutMs) {
        iterator.remove();
        currentSize.decrementAndGet();
        evicted++;

        try {
          producerEntry.producer.close(Duration.ofSeconds(30));
          log.debug("Evicted idle producer for topic: {}", entry.getKey());
        } catch (Exception e) {
          log.warn("Error closing evicted producer for topic: {}", entry.getKey(), e);
        }
      }
    }

    if (evicted > 0) {
      log.info("Evicted {} idle producers, pool size: {}/{}", evicted, currentSize.get(), maxPoolSize);
      if (metrics != null) {
        metrics.recordEvictions(evicted);
        metrics.recordPoolSize(currentSize.get());
      }
    }
  }

  @Override
  public int getCurrentSize() {
    return currentSize.get();
  }

  @Override
  public int getMaxSize() {
    return maxPoolSize;
  }

  @Override
  public void shutdown() {
    log.info("Shutting down producer pool");

    cleanupExecutor.shutdown();
    try {
      if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cleanupExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      cleanupExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    // Close all producers
    for (Map.Entry<String, ProducerEntry> entry : producers.entrySet()) {
      try {
        entry.getValue().producer.close(Duration.ofSeconds(30));
        log.debug("Closed producer for topic: {}", entry.getKey());
      } catch (Exception e) {
        log.warn("Error closing producer for topic: {}", entry.getKey(), e);
      }
    }
    producers.clear();
    currentSize.set(0);

    log.info("Producer pool shutdown complete");
  }
}
