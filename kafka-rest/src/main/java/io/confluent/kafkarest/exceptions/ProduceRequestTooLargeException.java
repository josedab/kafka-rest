/*
 * Copyright 2023 Confluent Inc.
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

/** This exception is thrown when a produce request exceeds the size threshold. */
public class ProduceRequestTooLargeException extends RuntimeException {

  private static final String DEFAULT_MESSAGE = "Produce request size is larger than allowed threshold";

  // Singleton for common case without stack trace
  private static final ProduceRequestTooLargeException STACKLESS_INSTANCE =
      new StacklessProduceRequestTooLargeException(DEFAULT_MESSAGE);

  public ProduceRequestTooLargeException() {
    this(DEFAULT_MESSAGE);
  }

  public ProduceRequestTooLargeException(String message) {
    super(message);
  }

  /**
   * Returns a stackless exception for better performance in common error paths.
   *
   * <p>This method returns a singleton instance without stack trace capture,
   * providing significant performance improvement for high-frequency produce
   * request size validation errors.
   *
   * @return a stackless ProduceRequestTooLargeException singleton
   */
  public static ProduceRequestTooLargeException withoutStackTrace() {
    return STACKLESS_INSTANCE;
  }

  /**
   * Creates a stackless exception with a custom message for better performance.
   *
   * @param message the error message
   * @return a new stackless ProduceRequestTooLargeException
   */
  public static ProduceRequestTooLargeException withoutStackTrace(String message) {
    return new StacklessProduceRequestTooLargeException(message);
  }

  /**
   * A ProduceRequestTooLargeException variant that skips stack trace capture for performance.
   * Used in high-frequency produce request validation paths.
   */
  private static class StacklessProduceRequestTooLargeException
      extends ProduceRequestTooLargeException {

    StacklessProduceRequestTooLargeException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
