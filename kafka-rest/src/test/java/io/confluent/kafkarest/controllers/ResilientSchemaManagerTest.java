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

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.RegisteredSchema;
import io.confluent.kafkarest.exceptions.SchemaRegistryCircuitBreakerException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ResilientSchemaManagerTest {

  private static final String TOPIC_NAME = "topic-1";
  private static final String SUBJECT = "topic-1-value";
  private static final int SCHEMA_ID = 1;
  private static final int SCHEMA_VERSION = 1;
  private static final AvroSchema SCHEMA = new AvroSchema("{\"type\": \"int\"}");

  private SchemaManager mockDelegate;
  private ResilientSchemaManager resilientSchemaManager;

  @BeforeEach
  public void setUp() {
    mockDelegate = mock(SchemaManager.class);

    // Create resilient schema manager with low thresholds for testing
    resilientSchemaManager =
        new ResilientSchemaManager(
            mockDelegate,
            /* failureRateThreshold= */ 50,
            /* waitDuration= */ Duration.ofMillis(100),
            /* halfOpenCalls= */ 2,
            /* slidingWindowSize= */ 4,
            /* minimumNumberOfCalls= */ 2,
            /* cacheSize= */ 100);
  }

  @Test
  public void getSchema_delegatesToUnderlyingManager() {
    RegisteredSchema expected =
        RegisteredSchema.create(SUBJECT, SCHEMA_ID, SCHEMA_VERSION, SCHEMA);

    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andReturn(expected);
    replay(mockDelegate);

    RegisteredSchema actual =
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID),
            Optional.empty(),
            Optional.empty(),
            false);

    assertEquals(expected, actual);
    verify(mockDelegate);
  }

  @Test
  public void getSchema_cachesResult() {
    RegisteredSchema expected =
        RegisteredSchema.create(SUBJECT, SCHEMA_ID, SCHEMA_VERSION, SCHEMA);

    // Delegate should only be called once
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andReturn(expected)
        .once();
    replay(mockDelegate);

    // First call should hit the delegate
    RegisteredSchema first =
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID),
            Optional.empty(),
            Optional.empty(),
            false);

    // Second call with same parameters should use cache
    RegisteredSchema second =
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID),
            Optional.empty(),
            Optional.empty(),
            false);

    assertEquals(expected, first);
    assertEquals(expected, second);
    assertEquals(1, resilientSchemaManager.getCacheSize());
    verify(mockDelegate);
  }

  @Test
  public void getSchema_doesNotCacheRawSchemaLookups() {
    RegisteredSchema expected =
        RegisteredSchema.create(SUBJECT, SCHEMA_ID, SCHEMA_VERSION, SCHEMA);

    // Delegate should be called twice since raw schema lookups are not cached
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andReturn(expected)
        .times(2);
    replay(mockDelegate);

    // First call with raw schema
    resilientSchemaManager.getSchema(
        TOPIC_NAME,
        Optional.of(EmbeddedFormat.AVRO),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of("{\"type\": \"int\"}"),
        false);

    // Second call with same raw schema should still hit delegate
    resilientSchemaManager.getSchema(
        TOPIC_NAME,
        Optional.of(EmbeddedFormat.AVRO),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of("{\"type\": \"int\"}"),
        false);

    verify(mockDelegate);
  }

  @Test
  public void getSchema_circuitBreakerOpensOnFailures() {
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andThrow(new RuntimeException(new IOException("Connection refused")))
        .times(4);
    replay(mockDelegate);

    // Make minimum calls to trigger circuit breaker evaluation
    for (int i = 0; i < 4; i++) {
      try {
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID + i), // Different schema IDs to avoid cache
            Optional.empty(),
            Optional.empty(),
            false);
      } catch (Exception ignored) {
        // Expected to fail
      }
    }

    // Circuit breaker should now be open
    assertEquals(CircuitBreaker.State.OPEN, resilientSchemaManager.getCircuitBreakerState());
  }

  @Test
  public void getSchema_throwsCircuitBreakerExceptionWhenOpen() throws Exception {
    // First, open the circuit breaker
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andThrow(new RuntimeException(new IOException("Connection refused")))
        .times(4);
    replay(mockDelegate);

    // Make calls to open the circuit
    for (int i = 0; i < 4; i++) {
      try {
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID + i),
            Optional.empty(),
            Optional.empty(),
            false);
      } catch (Exception ignored) {
      }
    }

    reset(mockDelegate);
    replay(mockDelegate);

    // Now verify that subsequent calls throw CircuitBreakerException
    assertThrows(
        SchemaRegistryCircuitBreakerException.class,
        () ->
            resilientSchemaManager.getSchema(
                TOPIC_NAME,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(100), // New schema ID, not in cache
                Optional.empty(),
                Optional.empty(),
                false));

    // Verify delegate was NOT called (circuit breaker rejected)
    verify(mockDelegate);
  }

  @Test
  public void getSchema_circuitBreakerTransitionsToHalfOpen() throws Exception {
    // Open the circuit breaker
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andThrow(new RuntimeException(new IOException("Connection refused")))
        .times(4);
    replay(mockDelegate);

    for (int i = 0; i < 4; i++) {
      try {
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID + i),
            Optional.empty(),
            Optional.empty(),
            false);
      } catch (Exception ignored) {
      }
    }

    assertEquals(CircuitBreaker.State.OPEN, resilientSchemaManager.getCircuitBreakerState());

    // Wait for the circuit breaker to transition to half-open
    Thread.sleep(150); // waitDuration is 100ms

    // Next call should transition to half-open
    reset(mockDelegate);
    RegisteredSchema expected =
        RegisteredSchema.create(SUBJECT, SCHEMA_ID, SCHEMA_VERSION, SCHEMA);
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andReturn(expected);
    replay(mockDelegate);

    resilientSchemaManager.getSchema(
        TOPIC_NAME,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(200),
        Optional.empty(),
        Optional.empty(),
        false);

    assertTrue(
        resilientSchemaManager.getCircuitBreakerState() == CircuitBreaker.State.HALF_OPEN
            || resilientSchemaManager.getCircuitBreakerState() == CircuitBreaker.State.CLOSED);
  }

  @Test
  public void getSchema_circuitBreakerClosesAfterSuccessfulHalfOpenCalls() throws Exception {
    // Open the circuit breaker
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andThrow(new RuntimeException(new IOException("Connection refused")))
        .times(4);
    replay(mockDelegate);

    for (int i = 0; i < 4; i++) {
      try {
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID + i),
            Optional.empty(),
            Optional.empty(),
            false);
      } catch (Exception ignored) {
      }
    }

    // Wait for half-open
    Thread.sleep(150);

    // Make successful calls in half-open state
    reset(mockDelegate);
    RegisteredSchema expected =
        RegisteredSchema.create(SUBJECT, SCHEMA_ID, SCHEMA_VERSION, SCHEMA);
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andReturn(expected)
        .times(2); // halfOpenCalls = 2
    replay(mockDelegate);

    for (int i = 0; i < 2; i++) {
      resilientSchemaManager.getSchema(
          TOPIC_NAME,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(300 + i),
          Optional.empty(),
          Optional.empty(),
          false);
    }

    // Circuit should now be closed
    assertEquals(CircuitBreaker.State.CLOSED, resilientSchemaManager.getCircuitBreakerState());
  }

  @Test
  public void getSchema_recordsRestClientExceptionAsFailure() {
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andThrow(new RuntimeException(new RestClientException("SR Error", 500, 50001)))
        .times(4);
    replay(mockDelegate);

    for (int i = 0; i < 4; i++) {
      try {
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID + i),
            Optional.empty(),
            Optional.empty(),
            false);
      } catch (Exception ignored) {
      }
    }

    // Circuit should be open due to RestClientException failures
    assertEquals(CircuitBreaker.State.OPEN, resilientSchemaManager.getCircuitBreakerState());
  }

  @Test
  public void resetCircuitBreaker_closesCircuit() throws Exception {
    // Open the circuit breaker
    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andThrow(new RuntimeException(new IOException("Connection refused")))
        .times(4);
    replay(mockDelegate);

    for (int i = 0; i < 4; i++) {
      try {
        resilientSchemaManager.getSchema(
            TOPIC_NAME,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(SCHEMA_ID + i),
            Optional.empty(),
            Optional.empty(),
            false);
      } catch (Exception ignored) {
      }
    }

    assertEquals(CircuitBreaker.State.OPEN, resilientSchemaManager.getCircuitBreakerState());

    // Reset the circuit breaker
    resilientSchemaManager.resetCircuitBreaker();

    assertEquals(CircuitBreaker.State.CLOSED, resilientSchemaManager.getCircuitBreakerState());
  }

  @Test
  public void invalidateCache_clearsAllCachedSchemas() {
    RegisteredSchema expected =
        RegisteredSchema.create(SUBJECT, SCHEMA_ID, SCHEMA_VERSION, SCHEMA);

    expect(
            mockDelegate.getSchema(
                anyString(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyObject(),
                anyBoolean()))
        .andReturn(expected)
        .times(2);
    replay(mockDelegate);

    // First call populates cache
    resilientSchemaManager.getSchema(
        TOPIC_NAME,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(SCHEMA_ID),
        Optional.empty(),
        Optional.empty(),
        false);

    assertEquals(1, resilientSchemaManager.getCacheSize());

    // Invalidate cache
    resilientSchemaManager.invalidateCache();

    assertEquals(0, resilientSchemaManager.getCacheSize());

    // Next call should hit delegate again
    resilientSchemaManager.getSchema(
        TOPIC_NAME,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(SCHEMA_ID),
        Optional.empty(),
        Optional.empty(),
        false);

    verify(mockDelegate);
  }

  @Test
  public void getCircuitBreakerMetrics_returnsMetrics() {
    CircuitBreaker.Metrics metrics = resilientSchemaManager.getCircuitBreakerMetrics();
    assertEquals(0, metrics.getNumberOfSuccessfulCalls());
    assertEquals(0, metrics.getNumberOfFailedCalls());
  }
}
