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

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import io.confluent.rest.exceptions.RestNotFoundException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for AsyncConsumerManager functionality.
 */
@ExtendWith(EasyMockExtension.class)
public class AsyncConsumerManagerTest {

  private static final String GROUP_NAME = "testgroup";
  private static final String TOPIC_NAME = "testtopic";

  private KafkaRestConfig config;

  @Mock
  private KafkaConsumerManager.KafkaConsumerFactory consumerFactory;

  private AsyncConsumerManager asyncConsumerManager;
  private MockConsumer<byte[], byte[]> consumer;
  private Capture<Properties> capturedConsumerConfig;

  @BeforeEach
  public void setUp() {
    Properties props = setUpProperties();
    config = new KafkaRestConfig(props);
    asyncConsumerManager = new AsyncConsumerManager(config, consumerFactory);
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST, GROUP_NAME);
  }

  private Properties setUpProperties() {
    Properties props = new Properties();
    props.setProperty(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://hostname:9092");
    props.setProperty(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG, "1024");
    props.setProperty(KafkaRestConfig.CONSUMER_ASYNC_ENABLED_CONFIG, "true");
    props.setProperty(KafkaRestConfig.CONSUMER_ASYNC_SCHEDULER_THREADS_CONFIG, "2");
    props.setProperty(KafkaRestConfig.CONSUMER_ASYNC_POLL_INTERVAL_MS_CONFIG, "10");
    props.setProperty(KafkaRestConfig.CONSUMER_ASYNC_MAX_PENDING_POLLS_CONFIG, "100");
    props.setProperty("consumer." + ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
    return props;
  }

  @AfterEach
  public void tearDown() {
    asyncConsumerManager.shutdown();
  }

  private void expectCreate(MockConsumer consumer) {
    capturedConsumerConfig = Capture.newInstance();
    Properties props = EasyMock.capture(capturedConsumerConfig);
    EasyMock.expect(consumerFactory.createConsumer(props)).andStubReturn(consumer);
    EasyMock.replay(consumerFactory);
  }

  @Test
  public void testCreateConsumer() {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    assertNotNull(instanceId);
    assertTrue(instanceId.startsWith("rest-consumer-"));
  }

  @Test
  public void testCreateConsumerWithCustomName() {
    expectCreate(consumer);

    ConsumerInstanceConfig instanceConfig = ConsumerInstanceConfig.create(
        null, "custom-consumer", EmbeddedFormat.BINARY, null, null, null, null);

    String instanceId = asyncConsumerManager.createConsumer(GROUP_NAME, instanceConfig);

    assertEquals("custom-consumer", instanceId);
  }

  @Test
  public void testCreateDuplicateConsumerThrows() {
    expectCreate(consumer);

    asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(
            null, "duplicate", EmbeddedFormat.BINARY, null, null, null, null));

    assertThrows(
        Exception.class,
        () -> asyncConsumerManager.createConsumer(
            GROUP_NAME, ConsumerInstanceConfig.create(
                null, "duplicate", EmbeddedFormat.BINARY, null, null, null, null)));
  }

  @Test
  public void testDeleteConsumer() {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    asyncConsumerManager.deleteConsumer(GROUP_NAME, instanceId);

    // Deleting again should throw
    assertThrows(RestNotFoundException.class,
        () -> asyncConsumerManager.deleteConsumer(GROUP_NAME, instanceId));
  }

  @Test
  public void testSubscribe() {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    ConsumerSubscriptionRecord subscription =
        new ConsumerSubscriptionRecord(Collections.singletonList(TOPIC_NAME), null);

    asyncConsumerManager.subscribe(GROUP_NAME, instanceId, subscription);

    // Verify subscription was successful by checking the subscription response
    var response = asyncConsumerManager.subscription(GROUP_NAME, instanceId);
    assertTrue(response.getTopics().contains(TOPIC_NAME));
  }

  @Test
  public void testUnsubscribe() {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    ConsumerSubscriptionRecord subscription =
        new ConsumerSubscriptionRecord(Collections.singletonList(TOPIC_NAME), null);

    asyncConsumerManager.subscribe(GROUP_NAME, instanceId, subscription);
    asyncConsumerManager.unsubscribe(GROUP_NAME, instanceId);

    // Subscription should be empty after unsubscribe
    var response = asyncConsumerManager.subscription(GROUP_NAME, instanceId);
    assertTrue(response.getTopics().isEmpty());
  }

  @Test
  public void testReadRecordsReturnsCompletableFuture()
      throws InterruptedException, ExecutionException, TimeoutException {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    asyncConsumerManager.subscribe(
        GROUP_NAME, instanceId,
        new ConsumerSubscriptionRecord(Collections.singletonList(TOPIC_NAME), null));

    consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
    consumer.updateBeginningOffsets(singletonMap(new TopicPartition(TOPIC_NAME, 0), 0L));

    // Add some records
    consumer.addRecord(createKafkaRecord(0));
    consumer.addRecord(createKafkaRecord(1));

    CompletableFuture<List<ConsumerRecord<ByteString, ByteString>>> future =
        asyncConsumerManager.readRecords(
            GROUP_NAME,
            instanceId,
            BinaryKafkaConsumerState.class,
            Duration.ofSeconds(5),
            Long.MAX_VALUE);

    List<ConsumerRecord<ByteString, ByteString>> records = future.get(10, TimeUnit.SECONDS);

    assertNotNull(records);
    assertEquals(2, records.size());
    assertEquals("k0", records.get(0).getKey().toStringUtf8());
    assertEquals("v0", records.get(0).getValue().toStringUtf8());
    assertEquals("k1", records.get(1).getKey().toStringUtf8());
    assertEquals("v1", records.get(1).getValue().toStringUtf8());
  }

  @Test
  public void testReadRecordsWithTimeout()
      throws InterruptedException, ExecutionException, TimeoutException {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    asyncConsumerManager.subscribe(
        GROUP_NAME, instanceId,
        new ConsumerSubscriptionRecord(Collections.singletonList(TOPIC_NAME), null));

    consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
    consumer.updateBeginningOffsets(singletonMap(new TopicPartition(TOPIC_NAME, 0), 0L));

    // Don't add any records - should timeout

    CompletableFuture<List<ConsumerRecord<ByteString, ByteString>>> future =
        asyncConsumerManager.readRecords(
            GROUP_NAME,
            instanceId,
            BinaryKafkaConsumerState.class,
            Duration.ofMillis(100),
            Long.MAX_VALUE);

    List<ConsumerRecord<ByteString, ByteString>> records = future.get(5, TimeUnit.SECONDS);

    assertNotNull(records);
    assertTrue(records.isEmpty());
  }

  @Test
  public void testReadRecordsNonExistentConsumerFails()
      throws InterruptedException, TimeoutException {
    CompletableFuture<List<ConsumerRecord<ByteString, ByteString>>> future =
        asyncConsumerManager.readRecords(
            GROUP_NAME,
            "nonexistent",
            BinaryKafkaConsumerState.class,
            Duration.ofSeconds(1),
            Long.MAX_VALUE);

    ExecutionException exception = assertThrows(
        ExecutionException.class,
        () -> future.get(5, TimeUnit.SECONDS));

    assertTrue(exception.getCause() instanceof RestNotFoundException);
  }

  @Test
  public void testCommitOffsetsAsync()
      throws InterruptedException, ExecutionException, TimeoutException {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    asyncConsumerManager.subscribe(
        GROUP_NAME, instanceId,
        new ConsumerSubscriptionRecord(Collections.singletonList(TOPIC_NAME), null));

    consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
    consumer.updateBeginningOffsets(singletonMap(new TopicPartition(TOPIC_NAME, 0), 0L));

    var future = asyncConsumerManager.commitOffsets(
        GROUP_NAME, instanceId, null, null);

    var offsets = future.get(10, TimeUnit.SECONDS);

    assertNotNull(offsets);
  }

  @Test
  public void testSeekToBeginning() {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    asyncConsumerManager.subscribe(
        GROUP_NAME, instanceId,
        new ConsumerSubscriptionRecord(Collections.singletonList(TOPIC_NAME), null));

    consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
    consumer.updateBeginningOffsets(singletonMap(new TopicPartition(TOPIC_NAME, 0), 0L));

    var seekRequest = new io.confluent.kafkarest.entities.v2.ConsumerSeekToRequest(
        Collections.singletonList(
            new io.confluent.kafkarest.entities.v2.TopicPartition(TOPIC_NAME, 0)));

    // Should not throw
    asyncConsumerManager.seekToBeginning(GROUP_NAME, instanceId, seekRequest);
  }

  @Test
  public void testSeekToEnd() {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    asyncConsumerManager.subscribe(
        GROUP_NAME, instanceId,
        new ConsumerSubscriptionRecord(Collections.singletonList(TOPIC_NAME), null));

    consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
    consumer.updateEndOffsets(singletonMap(new TopicPartition(TOPIC_NAME, 0), 100L));

    var seekRequest = new io.confluent.kafkarest.entities.v2.ConsumerSeekToRequest(
        Collections.singletonList(
            new io.confluent.kafkarest.entities.v2.TopicPartition(TOPIC_NAME, 0)));

    // Should not throw
    asyncConsumerManager.seekToEnd(GROUP_NAME, instanceId, seekRequest);
  }

  @Test
  public void testAssignPartitions() {
    expectCreate(consumer);

    String instanceId = asyncConsumerManager.createConsumer(
        GROUP_NAME, ConsumerInstanceConfig.create(EmbeddedFormat.BINARY));

    var assignmentRequest = new io.confluent.kafkarest.entities.v2.ConsumerAssignmentRequest(
        Collections.singletonList(
            new io.confluent.kafkarest.entities.v2.TopicPartition(TOPIC_NAME, 0)));

    asyncConsumerManager.assign(GROUP_NAME, instanceId, assignmentRequest);

    var response = asyncConsumerManager.assignment(GROUP_NAME, instanceId);
    assertEquals(1, response.getPartitions().size());
    assertEquals(TOPIC_NAME, response.getPartitions().get(0).getTopic());
    assertEquals(0, response.getPartitions().get(0).getPartition());
  }

  @Test
  public void testConfigurationProperties() {
    Properties props = setUpProperties();
    KafkaRestConfig testConfig = new KafkaRestConfig(props);

    assertTrue(testConfig.isAsyncConsumerEnabled());
    assertEquals(2, testConfig.getAsyncConsumerSchedulerThreads());
    assertEquals(10, testConfig.getAsyncConsumerPollIntervalMs());
    assertEquals(100, testConfig.getAsyncConsumerMaxPendingPolls());
  }

  @Test
  public void testAsyncConsumerDisabledByDefault() {
    Properties props = new Properties();
    props.setProperty(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://hostname:9092");
    KafkaRestConfig testConfig = new KafkaRestConfig(props);

    // By default, async consumers should be disabled
    assertEquals(false, testConfig.isAsyncConsumerEnabled());
  }

  private org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> createKafkaRecord(
      int offset) {
    return new org.apache.kafka.clients.consumer.ConsumerRecord<>(
        TOPIC_NAME,
        0,
        offset,
        String.format("k%d", offset).getBytes(),
        String.format("v%d", offset).getBytes());
  }
}
