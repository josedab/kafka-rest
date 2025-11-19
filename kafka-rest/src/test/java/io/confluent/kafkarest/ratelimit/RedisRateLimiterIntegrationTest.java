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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link RedisRateLimiter}.
 *
 * <p>These tests require a running Redis server. To run these tests:
 *
 * <pre>
 * 1. Start Redis: docker run -d -p 6379:6379 redis:7-alpine
 * 2. Run test: mvn test -Dtest=RedisRateLimiterIntegrationTest
 * </pre>
 *
 * <p>By default, these tests are disabled. Enable them by removing the @Disabled annotation when
 * Redis is available.
 */
@Disabled("Requires running Redis server. Start with: docker run -d -p 6379:6379 redis:7-alpine")
public class RedisRateLimiterIntegrationTest {

  private RedisRateLimiter rateLimiter;

  @BeforeEach
  void setUp() {
    rateLimiter =
        RedisRateLimiter.create(
            "localhost",
            6379,
            "",
            false,
            false,
            "",
            "test-kafka-rest-" + System.currentTimeMillis(),
            10, // 10 permits per second
            1, // 1 second window
            Duration.ofMillis(100));
  }

  @AfterEach
  void tearDown() {
    if (rateLimiter != null) {
      rateLimiter.close();
    }
  }

  @Test
  void testRateLimitingWithinLimit() {
    // Should allow up to 10 requests
    for (int i = 0; i < 10; i++) {
      assertDoesNotThrow(() -> rateLimiter.rateLimit(1));
    }
  }

  @Test
  void testRateLimitExceeded() {
    // Fill up the limit
    for (int i = 0; i < 10; i++) {
      rateLimiter.rateLimit(1);
    }

    // 11th request should be rate limited
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.rateLimit(1));
  }

  @Test
  void testCostParameter() {
    // Request with cost of 5
    rateLimiter.rateLimit(5);

    // Request with cost of 5
    rateLimiter.rateLimit(5);

    // Request with cost of 1 should fail
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.rateLimit(1));
  }

  @Test
  void testHigherCostThanLimit() {
    // Request with cost higher than remaining should fail
    rateLimiter.rateLimit(8);
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.rateLimit(5));
  }

  @Test
  void testSlidingWindow() throws InterruptedException {
    // Fill up the limit
    for (int i = 0; i < 10; i++) {
      rateLimiter.rateLimit(1);
    }

    // Should be rate limited
    assertThrows(RateLimitExceededException.class, () -> rateLimiter.rateLimit(1));

    // Wait for window to slide
    Thread.sleep(1100);

    // Should be able to make requests again
    assertDoesNotThrow(() -> rateLimiter.rateLimit(1));
  }

  @Test
  void testResilientRedisRateLimiter() {
    ResilientRedisRateLimiter resilientLimiter =
        ResilientRedisRateLimiter.create(
            "localhost",
            6379,
            "",
            false,
            false,
            "",
            "test-resilient-" + System.currentTimeMillis(),
            10,
            1,
            Duration.ofMillis(100),
            Duration.ofMillis(0),
            true,
            RateLimitBackend.GUAVA);

    try {
      // Should work normally
      for (int i = 0; i < 10; i++) {
        resilientLimiter.rateLimit(1);
      }

      // Should be rate limited
      assertThrows(RateLimitExceededException.class, () -> resilientLimiter.rateLimit(1));
    } finally {
      resilientLimiter.close();
    }
  }

  @Test
  void testCircuitBreakerState() {
    ResilientRedisRateLimiter resilientLimiter =
        ResilientRedisRateLimiter.create(
            "localhost",
            6379,
            "",
            false,
            false,
            "",
            "test-circuit-" + System.currentTimeMillis(),
            100,
            1,
            Duration.ofMillis(100),
            Duration.ofMillis(0),
            true,
            RateLimitBackend.GUAVA);

    try {
      // Circuit breaker should be closed initially
      resilientLimiter.rateLimit(1);
      // Circuit breaker state can be checked
      String state = resilientLimiter.getCircuitBreakerState();
      assertDoesNotThrow(() -> resilientLimiter.rateLimit(1));
    } finally {
      resilientLimiter.close();
    }
  }

  @Test
  void testMultipleKeyPrefixes() {
    // Test that different key prefixes create isolated rate limiters
    RedisRateLimiter limiter1 =
        RedisRateLimiter.create(
            "localhost", 6379, "", false, false, "", "prefix1-" + System.currentTimeMillis(),
            10, 1, Duration.ofMillis(100));

    RedisRateLimiter limiter2 =
        RedisRateLimiter.create(
            "localhost", 6379, "", false, false, "", "prefix2-" + System.currentTimeMillis(),
            10, 1, Duration.ofMillis(100));

    try {
      // Both limiters should have independent limits
      for (int i = 0; i < 10; i++) {
        limiter1.rateLimit(1);
        limiter2.rateLimit(1);
      }

      // Both should be rate limited independently
      assertThrows(RateLimitExceededException.class, () -> limiter1.rateLimit(1));
      assertThrows(RateLimitExceededException.class, () -> limiter2.rateLimit(1));
    } finally {
      limiter1.close();
      limiter2.close();
    }
  }
}
