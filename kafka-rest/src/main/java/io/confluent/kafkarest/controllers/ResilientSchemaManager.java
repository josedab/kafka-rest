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

package io.confluent.kafkarest.controllers;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.exceptions.SchemaRegistryCircuitBreakerException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resilient wrapper around {@link SchemaManager} that provides circuit breaker protection and
 * local caching for Schema Registry calls.
 *
 * <p>This implementation:
 *
 * <ul>
 *   <li>Uses a circuit breaker to prevent cascading failures when Schema Registry is unavailable
 *   <li>Caches schema lookups locally to reduce calls to Schema Registry
 *   <li>Fails fast when the circuit breaker is open
 * </ul>
 */
final class ResilientSchemaManager implements SchemaManager {

  private static final Logger log = LoggerFactory.getLogger(ResilientSchemaManager.class);

  private final SchemaManager delegate;
  private final CircuitBreaker circuitBreaker;
  private final Cache<SchemaKey, RegisteredSchema> localCache;

  ResilientSchemaManager(
      SchemaManager delegate,
      int failureRateThreshold,
      Duration waitDuration,
      int halfOpenCalls,
      int slidingWindowSize,
      int minimumNumberOfCalls,
      int cacheSize) {
    this.delegate = requireNonNull(delegate);

    CircuitBreakerConfig config =
        CircuitBreakerConfig.custom()
            .failureRateThreshold(failureRateThreshold)
            .minimumNumberOfCalls(minimumNumberOfCalls)
            .waitDurationInOpenState(waitDuration)
            .permittedNumberOfCallsInHalfOpenState(halfOpenCalls)
            .slidingWindowType(SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(slidingWindowSize)
            .recordExceptions(
                IOException.class, TimeoutException.class, RestClientException.class)
            .ignoreExceptions(
                io.confluent.kafkarest.exceptions.BadRequestException.class,
                io.confluent.rest.exceptions.RestNotFoundException.class)
            .build();

    this.circuitBreaker = CircuitBreaker.of("schema-registry", config);

    // Register event listeners for metrics and logging
    circuitBreaker
        .getEventPublisher()
        .onStateTransition(
            event ->
                log.info(
                    "Schema Registry circuit breaker state transition: {} -> {}",
                    event.getStateTransition().getFromState(),
                    event.getStateTransition().getToState()))
        .onSuccess(event -> log.debug("Schema Registry call succeeded"))
        .onError(
            event ->
                log.warn(
                    "Schema Registry call failed: {}",
                    event.getThrowable().getMessage(),
                    event.getThrowable()))
        .onCallNotPermitted(
            event ->
                log.warn(
                    "Schema Registry call not permitted - circuit breaker is open"));

    this.localCache = Caffeine.newBuilder().maximumSize(cacheSize).build();

    log.info(
        "ResilientSchemaManager initialized with circuit breaker config: "
            + "failureRateThreshold={}, waitDuration={}, halfOpenCalls={}, "
            + "slidingWindowSize={}, minimumNumberOfCalls={}, cacheSize={}",
        failureRateThreshold,
        waitDuration,
        halfOpenCalls,
        slidingWindowSize,
        minimumNumberOfCalls,
        cacheSize);
  }

  @Override
  public RegisteredSchema getSchema(
      String topicName,
      Optional<EmbeddedFormat> format,
      Optional<String> subject,
      Optional<SubjectNameStrategy> subjectNameStrategy,
      Optional<Integer> schemaId,
      Optional<Integer> schemaVersion,
      Optional<String> rawSchema,
      boolean isKey) {

    // Create cache key - only cache for schema ID and version lookups
    // Raw schema lookups may register new schemas, so we don't cache those
    SchemaKey cacheKey = null;
    if (rawSchema.isEmpty()) {
      cacheKey = SchemaKey.create(topicName, subject, schemaId, schemaVersion, isKey);
      RegisteredSchema cached = localCache.getIfPresent(cacheKey);
      if (cached != null) {
        log.debug("Cache hit for schema key: {}", cacheKey);
        return cached;
      }
    }

    // Call through circuit breaker
    try {
      final SchemaKey finalCacheKey = cacheKey;
      RegisteredSchema schema =
          circuitBreaker.executeSupplier(
              () ->
                  delegate.getSchema(
                      topicName,
                      format,
                      subject,
                      subjectNameStrategy,
                      schemaId,
                      schemaVersion,
                      rawSchema,
                      isKey));

      // Cache the result if we have a cache key
      if (finalCacheKey != null) {
        localCache.put(finalCacheKey, schema);
        log.debug("Cached schema for key: {}", finalCacheKey);
      }

      return schema;

    } catch (CallNotPermittedException e) {
      log.warn("Schema Registry circuit breaker is open, rejecting request");
      throw new SchemaRegistryCircuitBreakerException(e);
    }
  }

  /** Returns the current state of the circuit breaker. */
  public CircuitBreaker.State getCircuitBreakerState() {
    return circuitBreaker.getState();
  }

  /** Returns the circuit breaker metrics. */
  public CircuitBreaker.Metrics getCircuitBreakerMetrics() {
    return circuitBreaker.getMetrics();
  }

  /** Returns the number of cached schemas. */
  public long getCacheSize() {
    return localCache.estimatedSize();
  }

  /** Invalidates the entire cache. */
  public void invalidateCache() {
    localCache.invalidateAll();
    log.info("Schema cache invalidated");
  }

  /** Manually transitions the circuit breaker to closed state for testing or recovery. */
  public void resetCircuitBreaker() {
    circuitBreaker.reset();
    log.info("Circuit breaker reset to closed state");
  }
}
