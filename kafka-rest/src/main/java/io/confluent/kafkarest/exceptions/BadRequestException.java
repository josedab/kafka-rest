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

package io.confluent.kafkarest.exceptions;

import jakarta.ws.rs.core.Response.Status;

public class BadRequestException extends StatusCodeException {

  public BadRequestException(String detail) {
    this("Bad Request", detail);
  }

  public BadRequestException(String detail, Throwable cause) {
    this("Bad Request", detail, cause);
  }

  public BadRequestException(String title, String detail) {
    super(Status.BAD_REQUEST, title, detail);
  }

  public BadRequestException(String title, String detail, Throwable cause) {
    super(Status.BAD_REQUEST, title, detail, cause);
  }

  /**
   * Creates a stackless BadRequestException for better performance in common error paths.
   *
   * <p>Use this method when the exception is thrown for expected, high-frequency validation errors
   * where stack traces provide little debugging value. For unexpected errors or debugging,
   * use the regular constructors.
   *
   * @param detail the error detail message
   * @return a stackless BadRequestException
   */
  public static BadRequestException withoutStackTrace(String detail) {
    return new StacklessBadRequestException("Bad Request", detail);
  }

  /**
   * Creates a stackless BadRequestException with custom title for better performance.
   *
   * @param title the error title
   * @param detail the error detail message
   * @return a stackless BadRequestException
   */
  public static BadRequestException withoutStackTrace(String title, String detail) {
    return new StacklessBadRequestException(title, detail);
  }

  /**
   * A BadRequestException variant that skips stack trace capture for performance.
   * Used in high-frequency validation error paths.
   */
  private static class StacklessBadRequestException extends BadRequestException {

    StacklessBadRequestException(String title, String detail) {
      super(title, detail);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
