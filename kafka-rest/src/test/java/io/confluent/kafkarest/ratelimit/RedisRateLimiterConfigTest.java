/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.ratelimit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.KafkaRestConfig;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for Redis rate limiting configuration. */
public class RedisRateLimiterConfigTest {

  @Test
  public void testDefaultRedisConfiguration() {
    Properties props = new Properties();
    KafkaRestConfig config = new KafkaRestConfig(props);

    assertEquals("localhost", config.getRateLimitRedisHost());
    assertEquals(6379, config.getRateLimitRedisPort());
    assertEquals("", config.getRateLimitRedisPassword());
    assertFalse(config.isRateLimitRedisSslEnabled());
    assertFalse(config.isRateLimitRedisClusterEnabled());
    assertEquals("", config.getRateLimitRedisClusterNodes());
    assertEquals(1, config.getRateLimitRedisWindowSeconds());
    assertTrue(config.isRateLimitRedisFallbackEnabled());
    assertEquals(RateLimitBackend.GUAVA, config.getRateLimitRedisFallbackBackend());
    assertEquals(Duration.ofMillis(100), config.getRateLimitRedisTimeout());
    assertEquals("kafka-rest", config.getRateLimitRedisKeyPrefix());
  }

  @Test
  public void testCustomRedisConfiguration() {
    Properties props = new Properties();
    props.put("rate.limit.redis.host", "redis.example.com");
    props.put("rate.limit.redis.port", "6380");
    props.put("rate.limit.redis.password", "secret");
    props.put("rate.limit.redis.ssl.enabled", "true");
    props.put("rate.limit.redis.cluster.enabled", "true");
    props.put("rate.limit.redis.cluster.nodes", "host1:6379,host2:6379,host3:6379");
    props.put("rate.limit.redis.window.seconds", "5");
    props.put("rate.limit.redis.fallback.enabled", "false");
    props.put("rate.limit.redis.fallback.backend", "resilience4j");
    props.put("rate.limit.redis.timeout.ms", "200");
    props.put("rate.limit.redis.key.prefix", "my-app");

    KafkaRestConfig config = new KafkaRestConfig(props);

    assertEquals("redis.example.com", config.getRateLimitRedisHost());
    assertEquals(6380, config.getRateLimitRedisPort());
    assertEquals("secret", config.getRateLimitRedisPassword());
    assertTrue(config.isRateLimitRedisSslEnabled());
    assertTrue(config.isRateLimitRedisClusterEnabled());
    assertEquals("host1:6379,host2:6379,host3:6379", config.getRateLimitRedisClusterNodes());
    assertEquals(5, config.getRateLimitRedisWindowSeconds());
    assertFalse(config.isRateLimitRedisFallbackEnabled());
    assertEquals(RateLimitBackend.RESILIENCE4J, config.getRateLimitRedisFallbackBackend());
    assertEquals(Duration.ofMillis(200), config.getRateLimitRedisTimeout());
    assertEquals("my-app", config.getRateLimitRedisKeyPrefix());
  }

  @Test
  public void testRedisBackendEnum() {
    Properties props = new Properties();
    props.put("rate.limit.backend", "redis");

    KafkaRestConfig config = new KafkaRestConfig(props);

    assertEquals(RateLimitBackend.REDIS, config.getRateLimitBackend());
  }

  @Test
  public void testBackendDocumentation() {
    // Verify that the backend configuration properly accepts redis as an option
    Properties props = new Properties();
    props.put("rate.limit.backend", "REDIS");

    KafkaRestConfig config = new KafkaRestConfig(props);

    assertEquals(RateLimitBackend.REDIS, config.getRateLimitBackend());
  }
}
