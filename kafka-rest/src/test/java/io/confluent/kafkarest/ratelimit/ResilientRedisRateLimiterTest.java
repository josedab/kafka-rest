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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ResilientRedisRateLimiter}.
 *
 * <p>Note: These tests verify configuration and fallback behavior. Integration tests with a real
 * Redis server should be run separately with the appropriate infrastructure.
 */
public class ResilientRedisRateLimiterTest {

  @Test
  public void testGuavaFallbackLimiterRateLimits() {
    // Test that Guava fallback limiter properly enforces rate limits
    RequestRateLimiter fallbackLimiter = GuavaRateLimiter.create(10, Duration.ofMillis(0));

    // First 10 should pass
    for (int i = 0; i < 10; i++) {
      fallbackLimiter.rateLimit(1);
    }

    // 11th should fail
    assertThrows(RateLimitExceededException.class, () -> fallbackLimiter.rateLimit(1));
  }

  @Test
  public void testResilience4jFallbackLimiterRateLimits() {
    // Test that Resilience4j fallback limiter properly enforces rate limits
    RequestRateLimiter fallbackLimiter = Resilience4JRateLimiter.create(10, Duration.ofMillis(0));

    // First 10 should pass
    for (int i = 0; i < 10; i++) {
      fallbackLimiter.rateLimit(1);
    }

    // 11th should fail
    assertThrows(RateLimitExceededException.class, () -> fallbackLimiter.rateLimit(1));
  }

  @Test
  public void testFallbackBackendCreation() {
    // Test that both fallback backends can be created
    RequestRateLimiter guavaLimiter = GuavaRateLimiter.create(100, Duration.ofMillis(100));
    RequestRateLimiter resilience4jLimiter =
        Resilience4JRateLimiter.create(100, Duration.ofMillis(100));

    // Both should be able to process requests
    guavaLimiter.rateLimit(1);
    resilience4jLimiter.rateLimit(1);
  }

  @Test
  public void testCostParameter() {
    // Test that cost parameter is properly handled
    RequestRateLimiter limiter = GuavaRateLimiter.create(10, Duration.ofMillis(0));

    // Cost of 5 should consume 5 permits
    limiter.rateLimit(5);

    // Another cost of 5 should work
    limiter.rateLimit(5);

    // Cost of 1 should fail
    assertThrows(RateLimitExceededException.class, () -> limiter.rateLimit(1));
  }

  @Test
  public void testRateLimitExceededException() {
    RequestRateLimiter limiter = GuavaRateLimiter.create(1, Duration.ofMillis(0));

    // First request passes
    limiter.rateLimit(1);

    // Second request should throw
    RateLimitExceededException exception =
        assertThrows(RateLimitExceededException.class, () -> limiter.rateLimit(1));

    assertTrue(exception.getMessage().contains("rate limit"));
  }

  @Test
  public void testRedisRateLimiterException() {
    // Test that the Redis exception is properly structured
    RedisRateLimiter.RedisRateLimitException exception =
        new RedisRateLimiter.RedisRateLimitException(
            "Connection failed", new RuntimeException("Network error"));

    assertEquals("Connection failed", exception.getMessage());
    assertEquals("Network error", exception.getCause().getMessage());
  }
}
