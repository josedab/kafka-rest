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

package io.confluent.kafkarest.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.ratelimit.RateLimitExceededException;
import jakarta.ws.rs.core.Response.Status;
import org.junit.jupiter.api.Test;

/**
 * Tests for stackless exception variants to verify they correctly skip stack trace capture
 * for improved performance in high-frequency error paths.
 */
public class StacklessExceptionsTest {

  @Test
  public void badRequestException_withoutStackTrace_hasNoStackTrace() {
    BadRequestException exception = BadRequestException.withoutStackTrace("test detail");

    assertEquals(0, exception.getStackTrace().length);
    assertEquals("test detail", exception.getDetail());
    assertEquals("Bad Request", exception.getTitle());
    assertEquals(Status.BAD_REQUEST, exception.getStatus());
  }

  @Test
  public void badRequestException_withoutStackTrace_customTitle_hasNoStackTrace() {
    BadRequestException exception =
        BadRequestException.withoutStackTrace("Custom Title", "test detail");

    assertEquals(0, exception.getStackTrace().length);
    assertEquals("test detail", exception.getDetail());
    assertEquals("Custom Title", exception.getTitle());
    assertEquals(Status.BAD_REQUEST, exception.getStatus());
  }

  @Test
  public void badRequestException_normalConstructor_hasStackTrace() {
    BadRequestException exception = new BadRequestException("test detail");

    assertTrue(exception.getStackTrace().length > 0);
    assertEquals("test detail", exception.getDetail());
    assertEquals("Bad Request", exception.getTitle());
    assertEquals(Status.BAD_REQUEST, exception.getStatus());
  }

  @Test
  public void badRequestException_withoutStackTrace_isInstanceOfBadRequestException() {
    BadRequestException exception = BadRequestException.withoutStackTrace("test detail");

    assertTrue(exception instanceof BadRequestException);
    assertEquals(Status.BAD_REQUEST, exception.getStatus());
  }

  @Test
  public void produceRequestTooLargeException_withoutStackTrace_hasNoStackTrace() {
    ProduceRequestTooLargeException exception =
        ProduceRequestTooLargeException.withoutStackTrace();

    assertEquals(0, exception.getStackTrace().length);
    assertEquals("Produce request size is larger than allowed threshold", exception.getMessage());
  }

  @Test
  public void produceRequestTooLargeException_withoutStackTrace_returnsSingleton() {
    ProduceRequestTooLargeException exception1 =
        ProduceRequestTooLargeException.withoutStackTrace();
    ProduceRequestTooLargeException exception2 =
        ProduceRequestTooLargeException.withoutStackTrace();

    assertSame(exception1, exception2);
  }

  @Test
  public void produceRequestTooLargeException_withoutStackTrace_customMessage_hasNoStackTrace() {
    ProduceRequestTooLargeException exception =
        ProduceRequestTooLargeException.withoutStackTrace("custom message");

    assertEquals(0, exception.getStackTrace().length);
    assertEquals("custom message", exception.getMessage());
  }

  @Test
  public void produceRequestTooLargeException_normalConstructor_hasStackTrace() {
    ProduceRequestTooLargeException exception = new ProduceRequestTooLargeException();

    assertTrue(exception.getStackTrace().length > 0);
    assertEquals("Produce request size is larger than allowed threshold", exception.getMessage());
  }

  @Test
  public void produceRequestTooLargeException_withoutStackTrace_isInstanceOfProduceRequestTooLargeException() {
    ProduceRequestTooLargeException exception =
        ProduceRequestTooLargeException.withoutStackTrace();

    assertTrue(exception instanceof ProduceRequestTooLargeException);
  }

  @Test
  public void rateLimitExceededException_hasNoStackTrace() {
    // RateLimitExceededException is always stackless
    RateLimitExceededException exception = new RateLimitExceededException();

    assertEquals(0, exception.getStackTrace().length);
    assertEquals(Status.TOO_MANY_REQUESTS, exception.getStatus());
    assertEquals("Request rate limit exceeded", exception.getTitle());
  }

  @Test
  public void stacklessCompletionException_hasNoStackTrace() {
    RuntimeException cause = new RuntimeException("cause");
    StacklessCompletionException exception = new StacklessCompletionException(cause);

    assertEquals(0, exception.getStackTrace().length);
    assertSame(cause, exception.getCause());
  }

  @Test
  public void stacklessCompletionException_withMessage_hasNoStackTrace() {
    RuntimeException cause = new RuntimeException("cause");
    StacklessCompletionException exception =
        new StacklessCompletionException("custom message", cause);

    assertEquals(0, exception.getStackTrace().length);
    assertEquals("custom message", exception.getMessage());
    assertSame(cause, exception.getCause());
  }

  @Test
  public void stacklessExceptions_fillInStackTrace_returnsThis() {
    // Verify that fillInStackTrace returns the exception itself (for chaining)
    BadRequestException badRequest = BadRequestException.withoutStackTrace("test");
    assertNotNull(badRequest.fillInStackTrace());

    ProduceRequestTooLargeException produceTooLarge =
        ProduceRequestTooLargeException.withoutStackTrace();
    assertNotNull(produceTooLarge.fillInStackTrace());

    RateLimitExceededException rateLimit = new RateLimitExceededException();
    assertNotNull(rateLimit.fillInStackTrace());
  }
}
