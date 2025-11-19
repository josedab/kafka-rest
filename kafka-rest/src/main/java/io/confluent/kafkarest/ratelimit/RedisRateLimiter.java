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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RequestRateLimiter} implementation using Redis for distributed rate limiting.
 *
 * <p>Uses a sliding window algorithm with Redis sorted sets and Lua scripts for atomic operations.
 * This allows multiple REST Proxy instances to coordinate rate limiting.
 */
final class RedisRateLimiter extends RequestRateLimiter {
  private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);

  // Lua script for atomic rate limiting using sliding window
  private static final String RATE_LIMIT_SCRIPT =
      "local key = KEYS[1]\n"
          + "local limit = tonumber(ARGV[1])\n"
          + "local window = tonumber(ARGV[2])\n"
          + "local cost = tonumber(ARGV[3])\n"
          + "local now = tonumber(ARGV[4])\n"
          + "\n"
          + "-- Remove old entries outside the window\n"
          + "redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window * 1000)\n"
          + "\n"
          + "-- Count current entries in the window\n"
          + "local count = redis.call('ZCARD', key)\n"
          + "\n"
          + "if count + cost <= limit then\n"
          + "  -- Add new entries with current timestamp as score\n"
          + "  for i = 1, cost do\n"
          + "    redis.call('ZADD', key, now, now .. ':' .. math.random(1000000))\n"
          + "  end\n"
          + "  redis.call('EXPIRE', key, window * 2)\n"
          + "  return 1  -- Allowed\n"
          + "else\n"
          + "  return 0  -- Rejected\n"
          + "end";

  private final RedisClient standaloneClient;
  private final RedisClusterClient clusterClient;
  private final StatefulRedisConnection<String, String> standaloneConnection;
  private final StatefulRedisClusterConnection<String, String> clusterConnection;
  private final String keyPrefix;
  private final int permitsPerSecond;
  private final int windowSizeSeconds;
  private final Duration timeout;
  private final boolean isClusterMode;
  private volatile String scriptSha;

  private RedisRateLimiter(
      RedisClient standaloneClient,
      RedisClusterClient clusterClient,
      StatefulRedisConnection<String, String> standaloneConnection,
      StatefulRedisClusterConnection<String, String> clusterConnection,
      String keyPrefix,
      int permitsPerSecond,
      int windowSizeSeconds,
      Duration timeout,
      boolean isClusterMode) {
    this.standaloneClient = standaloneClient;
    this.clusterClient = clusterClient;
    this.standaloneConnection = standaloneConnection;
    this.clusterConnection = clusterConnection;
    this.keyPrefix = requireNonNull(keyPrefix);
    this.permitsPerSecond = permitsPerSecond;
    this.windowSizeSeconds = windowSizeSeconds;
    this.timeout = requireNonNull(timeout);
    this.isClusterMode = isClusterMode;
  }

  static RedisRateLimiter create(
      String host,
      int port,
      String password,
      boolean sslEnabled,
      boolean clusterEnabled,
      String clusterNodes,
      String keyPrefix,
      int permitsPerSecond,
      int windowSizeSeconds,
      Duration timeout) {

    if (clusterEnabled) {
      return createClusterClient(
          clusterNodes, password, sslEnabled, keyPrefix, permitsPerSecond, windowSizeSeconds,
          timeout);
    } else {
      return createStandaloneClient(
          host, port, password, sslEnabled, keyPrefix, permitsPerSecond, windowSizeSeconds,
          timeout);
    }
  }

  private static RedisRateLimiter createStandaloneClient(
      String host,
      int port,
      String password,
      boolean sslEnabled,
      String keyPrefix,
      int permitsPerSecond,
      int windowSizeSeconds,
      Duration timeout) {
    RedisURI.Builder uriBuilder =
        RedisURI.builder().withHost(host).withPort(port).withTimeout(timeout);

    if (password != null && !password.isEmpty()) {
      uriBuilder.withPassword(password.toCharArray());
    }

    if (sslEnabled) {
      uriBuilder.withSsl(true);
    }

    RedisClient client = RedisClient.create(uriBuilder.build());
    StatefulRedisConnection<String, String> connection = client.connect();

    log.info("Connected to Redis at {}:{} for distributed rate limiting", host, port);

    return new RedisRateLimiter(
        client,
        null,
        connection,
        null,
        keyPrefix,
        permitsPerSecond,
        windowSizeSeconds,
        timeout,
        false);
  }

  private static RedisRateLimiter createClusterClient(
      String clusterNodes,
      String password,
      boolean sslEnabled,
      String keyPrefix,
      int permitsPerSecond,
      int windowSizeSeconds,
      Duration timeout) {
    List<RedisURI> nodes =
        Arrays.stream(clusterNodes.split(","))
            .map(String::trim)
            .map(
                node -> {
                  String[] parts = node.split(":");
                  RedisURI.Builder builder =
                      RedisURI.builder()
                          .withHost(parts[0])
                          .withPort(parts.length > 1 ? Integer.parseInt(parts[1]) : 6379)
                          .withTimeout(timeout);

                  if (password != null && !password.isEmpty()) {
                    builder.withPassword(password.toCharArray());
                  }

                  if (sslEnabled) {
                    builder.withSsl(true);
                  }

                  return builder.build();
                })
            .toList();

    RedisClusterClient client = RedisClusterClient.create(nodes);
    StatefulRedisClusterConnection<String, String> connection = client.connect();

    log.info("Connected to Redis cluster at {} for distributed rate limiting", clusterNodes);

    return new RedisRateLimiter(
        null,
        client,
        null,
        connection,
        keyPrefix,
        permitsPerSecond,
        windowSizeSeconds,
        timeout,
        true);
  }

  @Override
  public void rateLimit(int cost) {
    String key = keyPrefix + ":ratelimit";
    long now = System.currentTimeMillis();
    int limit = permitsPerSecond * windowSizeSeconds;

    try {
      Long result = executeRateLimitScript(key, limit, windowSizeSeconds, cost, now);

      if (result == null || result == 0) {
        throw new RateLimitExceededException();
      }
    } catch (RateLimitExceededException e) {
      throw e;
    } catch (Exception e) {
      log.error("Redis rate limiter error", e);
      throw new RedisRateLimitException("Redis rate limiter failed", e);
    }
  }

  private Long executeRateLimitScript(String key, int limit, int window, int cost, long now)
      throws ExecutionException, InterruptedException, TimeoutException {
    String[] keys = {key};
    String[] args = {
      String.valueOf(limit), String.valueOf(window), String.valueOf(cost), String.valueOf(now)
    };

    if (isClusterMode) {
      RedisAdvancedClusterAsyncCommands<String, String> commands = clusterConnection.async();
      RedisFuture<Long> future =
          commands.eval(RATE_LIMIT_SCRIPT, ScriptOutputType.INTEGER, keys, args);
      return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } else {
      RedisAsyncCommands<String, String> commands = standaloneConnection.async();
      RedisFuture<Long> future =
          commands.eval(RATE_LIMIT_SCRIPT, ScriptOutputType.INTEGER, keys, args);
      return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  /** Closes the Redis connection and client. */
  public void close() {
    if (standaloneConnection != null) {
      standaloneConnection.close();
    }
    if (clusterConnection != null) {
      clusterConnection.close();
    }
    if (standaloneClient != null) {
      standaloneClient.shutdown();
    }
    if (clusterClient != null) {
      clusterClient.shutdown();
    }
    log.info("Redis rate limiter closed");
  }

  /** Exception thrown when Redis operations fail. */
  public static class RedisRateLimitException extends RuntimeException {
    public RedisRateLimitException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
