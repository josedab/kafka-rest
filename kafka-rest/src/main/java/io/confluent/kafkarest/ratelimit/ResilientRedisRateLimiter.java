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

import static java.util.Objects.requireNonNull;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resilient wrapper around {@link RedisRateLimiter} that provides circuit breaker and fallback
 * support.
 *
 * <p>When Redis is unavailable, this rate limiter falls back to a local rate limiter (Guava or
 * Resilience4j) to maintain service availability.
 */
final class ResilientRedisRateLimiter extends RequestRateLimiter {
  private static final Logger log = LoggerFactory.getLogger(ResilientRedisRateLimiter.class);

  private final RedisRateLimiter redisLimiter;
  private final RequestRateLimiter fallbackLimiter;
  private final CircuitBreaker circuitBreaker;
  private final boolean fallbackEnabled;
  private final AtomicLong redisFailureCount = new AtomicLong(0);
  private final AtomicLong fallbackUsedCount = new AtomicLong(0);

  private ResilientRedisRateLimiter(
      RedisRateLimiter redisLimiter,
      RequestRateLimiter fallbackLimiter,
      CircuitBreaker circuitBreaker,
      boolean fallbackEnabled) {
    this.redisLimiter = requireNonNull(redisLimiter);
    this.fallbackLimiter = fallbackLimiter;
    this.circuitBreaker = requireNonNull(circuitBreaker);
    this.fallbackEnabled = fallbackEnabled;
  }

  static ResilientRedisRateLimiter create(
      String host,
      int port,
      String password,
      boolean sslEnabled,
      boolean clusterEnabled,
      String clusterNodes,
      String keyPrefix,
      int permitsPerSecond,
      int windowSizeSeconds,
      Duration redisTimeout,
      Duration rateLimitTimeout,
      boolean fallbackEnabled,
      RateLimitBackend fallbackBackend) {

    RedisRateLimiter redisLimiter =
        RedisRateLimiter.create(
            host,
            port,
            password,
            sslEnabled,
            clusterEnabled,
            clusterNodes,
            keyPrefix,
            permitsPerSecond,
            windowSizeSeconds,
            redisTimeout);

    RequestRateLimiter fallbackLimiter = null;
    if (fallbackEnabled) {
      fallbackLimiter = createFallbackLimiter(fallbackBackend, permitsPerSecond, rateLimitTimeout);
      log.info(
          "Redis rate limiter fallback enabled using {} backend",
          fallbackBackend.name().toLowerCase());
    }

    CircuitBreaker circuitBreaker = createCircuitBreaker();

    return new ResilientRedisRateLimiter(
        redisLimiter, fallbackLimiter, circuitBreaker, fallbackEnabled);
  }

  private static RequestRateLimiter createFallbackLimiter(
      RateLimitBackend backend, int permitsPerSecond, Duration timeout) {
    switch (backend) {
      case GUAVA:
        return GuavaRateLimiter.create(permitsPerSecond, timeout);
      case RESILIENCE4J:
        return Resilience4JRateLimiter.create(permitsPerSecond, timeout);
      default:
        throw new IllegalArgumentException("Unsupported fallback backend: " + backend);
    }
  }

  private static CircuitBreaker createCircuitBreaker() {
    CircuitBreakerConfig config =
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50) // Open circuit after 50% failures
            .slowCallRateThreshold(100) // Consider slow calls as failures
            .slowCallDurationThreshold(Duration.ofMillis(500))
            .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait before trying again
            .permittedNumberOfCallsInHalfOpenState(3) // Number of test calls in half-open
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(10) // Number of calls to evaluate
            .minimumNumberOfCalls(5) // Minimum calls before evaluating
            .build();

    return CircuitBreaker.of("redis-rate-limiter", config);
  }

  @Override
  public void rateLimit(int cost) {
    try {
      circuitBreaker.executeRunnable(() -> redisLimiter.rateLimit(cost));
    } catch (RateLimitExceededException e) {
      // Rate limit exceeded is a valid response, not a failure
      throw e;
    } catch (Exception e) {
      redisFailureCount.incrementAndGet();
      log.warn("Redis rate limiter failed, using fallback", e);

      if (fallbackEnabled && fallbackLimiter != null) {
        fallbackUsedCount.incrementAndGet();
        fallbackLimiter.rateLimit(cost);
      } else {
        // If fallback is disabled, fail open (allow the request)
        log.warn("Redis rate limiter failed and fallback is disabled, allowing request");
      }
    }
  }

  /** Returns the number of Redis failures. */
  public long getRedisFailureCount() {
    return redisFailureCount.get();
  }

  /** Returns the number of times fallback was used. */
  public long getFallbackUsedCount() {
    return fallbackUsedCount.get();
  }

  /** Returns the current circuit breaker state. */
  public String getCircuitBreakerState() {
    return circuitBreaker.getState().name();
  }

  /** Closes the underlying Redis connection. */
  public void close() {
    if (redisLimiter != null) {
      redisLimiter.close();
    }
  }
}
