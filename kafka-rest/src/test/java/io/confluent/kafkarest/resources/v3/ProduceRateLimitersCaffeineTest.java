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

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.confluent.kafkarest.ratelimit.RequestRateLimiter;
import jakarta.inject.Provider;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for ProduceRateLimiters Caffeine cache implementation.
 * These tests verify cache statistics, concurrent access, and cache behavior.
 */
public class ProduceRateLimitersCaffeineTest {

  @Test
  public void test_cacheStatsAreRecorded() {
    // Setup
    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    expect(countLimitProvider.get()).andReturn(rateLimiterForCount).anyTimes();
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes).anyTimes();
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal).anyTimes();
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal).anyTimes();
    rateLimiterForCount.rateLimit(anyInt());
    rateLimiterForBytes.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    countLimiterGlobal.rateLimit(anyInt());

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            true,
            Duration.ofHours(1));

    // Act - make one request to populate cache
    produceRateLimiters.rateLimit("cluster1", 100L, mockRequest);

    // Assert - verify cache stats are available
    CacheStats countStats = produceRateLimiters.getCountCacheStats();
    CacheStats bytesStats = produceRateLimiters.getBytesCacheStats();

    assertNotNull(countStats);
    assertNotNull(bytesStats);
    assertEquals(1, countStats.missCount());
    assertEquals(1, bytesStats.missCount());
  }

  @Test
  public void test_cacheHitsAreRecorded() {
    // Setup
    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes = mock(RequestRateLimiter.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    // First request creates cache entries
    expect(countLimitProvider.get()).andReturn(rateLimiterForCount);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes);
    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal).anyTimes();
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal).anyTimes();
    rateLimiterForCount.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();
    rateLimiterForBytes.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();
    bytesLimiterGlobal.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();
    countLimiterGlobal.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount,
        rateLimiterForBytes,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            true,
            Duration.ofHours(1));

    // Act - make two requests to same cluster
    produceRateLimiters.rateLimit("cluster1", 100L, mockRequest);
    produceRateLimiters.rateLimit("cluster1", 100L, mockRequest);

    // Assert - first request is miss, second should be hit
    CacheStats countStats = produceRateLimiters.getCountCacheStats();
    CacheStats bytesStats = produceRateLimiters.getBytesCacheStats();

    assertEquals(1, countStats.missCount());
    assertEquals(1, countStats.hitCount());
    assertEquals(1, bytesStats.missCount());
    assertEquals(1, bytesStats.hitCount());
  }

  @Test
  public void test_concurrentAccessDoesNotBlock() throws Exception {
    // Setup
    AtomicInteger countCreations = new AtomicInteger(0);
    AtomicInteger bytesCreations = new AtomicInteger(0);

    Provider<RequestRateLimiter> countLimitProvider = () -> {
      countCreations.incrementAndGet();
      RequestRateLimiter limiter = mock(RequestRateLimiter.class);
      limiter.rateLimit(anyInt());
      replay(limiter);
      return limiter;
    };

    Provider<RequestRateLimiter> bytesLimitProvider = () -> {
      bytesCreations.incrementAndGet();
      RequestRateLimiter limiter = mock(RequestRateLimiter.class);
      limiter.rateLimit(anyInt());
      replay(limiter);
      return limiter;
    };

    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);

    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal).anyTimes();
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal).anyTimes();
    countLimiterGlobal.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();
    bytesLimiterGlobal.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();

    replay(
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            true,
            Duration.ofHours(1));

    // Act - run concurrent requests
    int numThreads = 10;
    int requestsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    List<Exception> exceptions = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          HttpServletRequest mockReq = mock(HttpServletRequest.class);
          replay(mockReq);
          for (int j = 0; j < requestsPerThread; j++) {
            String clusterId = "cluster-" + (threadId % 5); // 5 different clusters
            produceRateLimiters.rateLimit(clusterId, 100L, mockReq);
          }
        } catch (Exception e) {
          synchronized (exceptions) {
            exceptions.add(e);
          }
        } finally {
          latch.countDown();
        }
      });
    }

    boolean completed = latch.await(30, TimeUnit.SECONDS);
    executor.shutdown();

    // Assert
    assertTrue(completed, "All threads should complete within timeout");
    assertTrue(exceptions.isEmpty(), "No exceptions should occur: " + exceptions);

    // Should only create 5 rate limiters (one per cluster)
    assertEquals(5, countCreations.get(), "Should create exactly 5 count limiters");
    assertEquals(5, bytesCreations.get(), "Should create exactly 5 bytes limiters");

    // Verify cache stats show mostly hits
    CacheStats countStats = produceRateLimiters.getCountCacheStats();
    assertTrue(countStats.hitCount() > countStats.missCount(),
        "Cache hit rate should be high");
  }

  @Test
  public void test_clearInvalidatesCache() {
    // Setup
    Provider<RequestRateLimiter> countLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimitProvider = mock(Provider.class);
    Provider<RequestRateLimiter> countLimiterGlobalProvider = mock(Provider.class);
    Provider<RequestRateLimiter> bytesLimiterGlobalProvider = mock(Provider.class);
    RequestRateLimiter countLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter bytesLimiterGlobal = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes1 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForCount2 = mock(RequestRateLimiter.class);
    RequestRateLimiter rateLimiterForBytes2 = mock(RequestRateLimiter.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);

    // First request
    expect(countLimitProvider.get()).andReturn(rateLimiterForCount1);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes1);
    // Second request after clear
    expect(countLimitProvider.get()).andReturn(rateLimiterForCount2);
    expect(bytesLimitProvider.get()).andReturn(rateLimiterForBytes2);

    expect(countLimiterGlobalProvider.get()).andReturn(countLimiterGlobal).anyTimes();
    expect(bytesLimiterGlobalProvider.get()).andReturn(bytesLimiterGlobal).anyTimes();

    rateLimiterForCount1.rateLimit(anyInt());
    rateLimiterForBytes1.rateLimit(anyInt());
    rateLimiterForCount2.rateLimit(anyInt());
    rateLimiterForBytes2.rateLimit(anyInt());
    bytesLimiterGlobal.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();
    countLimiterGlobal.rateLimit(anyInt());
    org.easymock.EasyMock.expectLastCall().anyTimes();

    replay(
        countLimitProvider,
        bytesLimitProvider,
        rateLimiterForCount1,
        rateLimiterForBytes1,
        rateLimiterForCount2,
        rateLimiterForBytes2,
        countLimiterGlobal,
        bytesLimiterGlobal,
        countLimiterGlobalProvider,
        bytesLimiterGlobalProvider,
        mockRequest);

    ProduceRateLimiters produceRateLimiters =
        new ProduceRateLimiters(
            countLimitProvider,
            bytesLimitProvider,
            countLimiterGlobalProvider,
            bytesLimiterGlobalProvider,
            true,
            Duration.ofHours(1));

    // Act
    produceRateLimiters.rateLimit("cluster1", 100L, mockRequest);
    produceRateLimiters.clear();
    produceRateLimiters.rateLimit("cluster1", 100L, mockRequest);

    // Assert - both requests should be misses (second because cache was cleared)
    CacheStats countStats = produceRateLimiters.getCountCacheStats();
    assertEquals(2, countStats.missCount());
    assertEquals(0, countStats.hitCount());
  }
}
