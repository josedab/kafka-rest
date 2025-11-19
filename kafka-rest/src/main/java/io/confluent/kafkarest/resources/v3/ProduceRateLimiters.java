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

package io.confluent.kafkarest.resources.v3;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitCacheExpiryConfig;
import io.confluent.kafkarest.config.ConfigModule.ProduceRateLimitEnabledConfig;
import io.confluent.kafkarest.ratelimit.RateLimitExceededException;
import io.confluent.kafkarest.ratelimit.RateLimitExceededException.ErrorCodes;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterBytes;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterBytesGlobal;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterCount;
import io.confluent.kafkarest.ratelimit.RateLimitModule.ProduceRateLimiterCountGlobal;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import io.confluent.kafkarest.requestlog.CustomLogRequestAttributes;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Duration;

public class ProduceRateLimiters {

  private static final long DEFAULT_CACHE_MAXIMUM_SIZE = 10000;

  private final boolean rateLimitingEnabled;
  private final LoadingCache<String, RequestRateLimiter> countCache;
  private final LoadingCache<String, RequestRateLimiter> bytesCache;
  private final Provider<RequestRateLimiter> bytesLimiterGlobal;
  private final Provider<RequestRateLimiter> countLimiterGlobal;

  @Inject
  public ProduceRateLimiters(
      @ProduceRateLimiterCount Provider<RequestRateLimiter> countLimiterProvider,
      @ProduceRateLimiterBytes Provider<RequestRateLimiter> bytesLimiterProvider,
      @ProduceRateLimiterCountGlobal Provider<RequestRateLimiter> countLimiterGlobal,
      @ProduceRateLimiterBytesGlobal Provider<RequestRateLimiter> bytesLimiterGlobal,
      @ProduceRateLimitEnabledConfig Boolean produceRateLimitEnabledConfig,
      @ProduceRateLimitCacheExpiryConfig Duration produceRateLimitCacheExpiryConfig) {
    this.rateLimitingEnabled = requireNonNull(produceRateLimitEnabledConfig);
    this.countLimiterGlobal = requireNonNull(countLimiterGlobal);
    this.bytesLimiterGlobal = requireNonNull(bytesLimiterGlobal);

    countCache =
        Caffeine.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig)
            .maximumSize(DEFAULT_CACHE_MAXIMUM_SIZE)
            .recordStats()
            .build(key -> countLimiterProvider.get());
    bytesCache =
        Caffeine.newBuilder()
            .expireAfterAccess(produceRateLimitCacheExpiryConfig)
            .maximumSize(DEFAULT_CACHE_MAXIMUM_SIZE)
            .recordStats()
            .build(key -> bytesLimiterProvider.get());
  }

  public void rateLimit(String clusterId, long requestSize, HttpServletRequest httpServletRequest) {
    if (!rateLimitingEnabled) {
      return;
    }

    // Apply global rate-limits
    try {
      // Global rate limit first to reduce CPU usage under load
      // https://confluentinc.atlassian.net/browse/KREST-4979
      countLimiterGlobal.get().rateLimit(1);
    } catch (RateLimitExceededException ex) {
      httpServletRequest.setAttribute(
          CustomLogRequestAttributes.REST_ERROR_CODE,
          ErrorCodes.PRODUCE_MAX_REQUESTS_GLOBAL_LIMIT_EXCEEDED);
      throw ex;
    }
    try {
      bytesLimiterGlobal.get().rateLimit(toIntExact(requestSize));
    } catch (RateLimitExceededException ex) {
      httpServletRequest.setAttribute(
          CustomLogRequestAttributes.REST_ERROR_CODE,
          ErrorCodes.PRODUCE_MAX_BYTES_GLOBAL_LIMIT_EXCEEDED);
      throw ex;
    }

    // Apply tenant specific rate-limits
    RequestRateLimiter countRateLimiter = countCache.get(clusterId);
    RequestRateLimiter byteRateLimiter = bytesCache.get(clusterId);
    try {
      countRateLimiter.rateLimit(1);
    } catch (RateLimitExceededException ex) {
      httpServletRequest.setAttribute(
          CustomLogRequestAttributes.REST_ERROR_CODE,
          ErrorCodes.PRODUCE_MAX_REQUESTS_PER_TENANT_LIMIT_EXCEEDED);
      throw ex;
    }
    try {
      byteRateLimiter.rateLimit(toIntExact(requestSize));
    } catch (RateLimitExceededException ex) {
      httpServletRequest.setAttribute(
          CustomLogRequestAttributes.REST_ERROR_CODE,
          ErrorCodes.PRODUCE_MAX_BYTES_PER_TENANT_LIMIT_EXCEEDED);
      throw ex;
    }
  }

  public void clear() {
    countCache.invalidateAll();
    bytesCache.invalidateAll();
  }

  /**
   * Returns the cache statistics for the count limiter cache.
   */
  public CacheStats getCountCacheStats() {
    return countCache.stats();
  }

  /**
   * Returns the cache statistics for the bytes limiter cache.
   */
  public CacheStats getBytesCacheStats() {
    return bytesCache.stats();
  }
}
