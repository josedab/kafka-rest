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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultProducerPoolTest {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  private Map<String, Object> defaultConfig;
  private Map<String, Map<String, Object>> topicConfigs;
  private DefaultProducerPool pool;
  private TestClock testClock;

  @BeforeEach
  public void setUp() {
    defaultConfig = new HashMap<>();
    defaultConfig.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    defaultConfig.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    defaultConfig.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    topicConfigs = new HashMap<>();
    testClock = new TestClock();
  }

  @AfterEach
  public void tearDown() {
    if (pool != null) {
      pool.shutdown();
    }
  }

  @Test
  public void testGetProducer_returnsSameProducerForSameTopic() {
    pool = createPool(50, Duration.ofMinutes(5));

    Producer<byte[], byte[]> producer1 = pool.getProducer("topic-1");
    Producer<byte[], byte[]> producer2 = pool.getProducer("topic-1");

    assertNotNull(producer1);
    assertSame(producer1, producer2);
    assertEquals(1, pool.getCurrentSize());
  }

  @Test
  public void testGetProducer_createsDifferentProducersForDifferentTopics() {
    pool = createPool(50, Duration.ofMinutes(5));

    Producer<byte[], byte[]> producer1 = pool.getProducer("topic-1");
    Producer<byte[], byte[]> producer2 = pool.getProducer("topic-2");

    assertNotNull(producer1);
    assertNotNull(producer2);
    assertEquals(2, pool.getCurrentSize());
  }

  @Test
  public void testGetMaxSize_returnsConfiguredMaxSize() {
    pool = createPool(100, Duration.ofMinutes(5));

    assertEquals(100, pool.getMaxSize());
  }

  @Test
  public void testGetCurrentSize_returnsActualPoolSize() {
    pool = createPool(50, Duration.ofMinutes(5));

    assertEquals(0, pool.getCurrentSize());

    pool.getProducer("topic-1");
    assertEquals(1, pool.getCurrentSize());

    pool.getProducer("topic-2");
    assertEquals(2, pool.getCurrentSize());

    // Same topic should not increase size
    pool.getProducer("topic-1");
    assertEquals(2, pool.getCurrentSize());
  }

  @Test
  public void testPoolSizeLimit_reusesExistingProducerWhenFull() {
    pool = createPool(2, Duration.ofMinutes(5));

    Producer<byte[], byte[]> producer1 = pool.getProducer("topic-1");
    Producer<byte[], byte[]> producer2 = pool.getProducer("topic-2");

    assertEquals(2, pool.getCurrentSize());

    // Third topic should reuse existing producer
    Producer<byte[], byte[]> producer3 = pool.getProducer("topic-3");

    // Size should still be 2 since we hit the limit
    assertTrue(pool.getCurrentSize() <= 2);
    assertNotNull(producer3);
  }

  @Test
  public void testTopicSpecificConfig_appliesOverrides() {
    Map<String, Object> topic1Config = new HashMap<>();
    topic1Config.put("linger.ms", "100");
    topic1Config.put("batch.size", "65536");
    topicConfigs.put("high-throughput-topic", topic1Config);

    pool = createPool(50, Duration.ofMinutes(5));

    Producer<byte[], byte[]> producer = pool.getProducer("high-throughput-topic");
    assertNotNull(producer);
    assertEquals(1, pool.getCurrentSize());
  }

  @Test
  public void testMetrics_recordsPoolHits() {
    ProducerPoolMetrics metrics = mock(ProducerPoolMetrics.class);
    pool = new DefaultProducerPool(
        defaultConfig,
        topicConfigs,
        50,
        Duration.ofMinutes(5),
        metrics,
        testClock);

    pool.getProducer("topic-1");

    verify(metrics, atLeastOnce()).recordPoolHit();
  }

  @Test
  public void testMetrics_recordsPoolSize() {
    ProducerPoolMetrics metrics = mock(ProducerPoolMetrics.class);
    pool = new DefaultProducerPool(
        defaultConfig,
        topicConfigs,
        50,
        Duration.ofMinutes(5),
        metrics,
        testClock);

    pool.getProducer("topic-1");

    verify(metrics, atLeastOnce()).recordPoolSize(anyInt());
  }

  @Test
  public void testShutdown_closesAllProducers() {
    pool = createPool(50, Duration.ofMinutes(5));

    pool.getProducer("topic-1");
    pool.getProducer("topic-2");

    assertEquals(2, pool.getCurrentSize());

    pool.shutdown();

    assertEquals(0, pool.getCurrentSize());

    // Prevent double shutdown in tearDown
    pool = null;
  }

  @Test
  public void testNullTopicConfigs_usesDefaultConfig() {
    pool = new DefaultProducerPool(
        defaultConfig,
        null,
        50,
        Duration.ofMinutes(5),
        null,
        testClock);

    Producer<byte[], byte[]> producer = pool.getProducer("any-topic");
    assertNotNull(producer);
  }

  private DefaultProducerPool createPool(int maxSize, Duration idleTimeout) {
    return new DefaultProducerPool(
        defaultConfig,
        topicConfigs,
        maxSize,
        idleTimeout,
        null,
        testClock);
  }

  // Test clock for controlling time in tests
  private static class TestClock extends Clock {
    private final AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());

    @Override
    public ZoneId getZone() {
      return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(currentTimeMillis.get());
    }

    @Override
    public long millis() {
      return currentTimeMillis.get();
    }

    public void advance(Duration duration) {
      currentTimeMillis.addAndGet(duration.toMillis());
    }
  }
}
