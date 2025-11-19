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

import jakarta.ws.rs.core.Response.Status;

/**
 * Exception thrown when the Schema Registry circuit breaker is open, indicating that Schema
 * Registry is unavailable and the circuit breaker is preventing further calls.
 */
public final class SchemaRegistryCircuitBreakerException extends StatusCodeException {

  public static final int ERROR_CODE = 503001;

  public SchemaRegistryCircuitBreakerException() {
    super(
        Status.SERVICE_UNAVAILABLE,
        ERROR_CODE,
        "Schema Registry Unavailable",
        "Schema Registry circuit breaker is open. The service is temporarily unavailable.");
  }

  public SchemaRegistryCircuitBreakerException(Throwable cause) {
    super(
        Status.SERVICE_UNAVAILABLE,
        ERROR_CODE,
        "Schema Registry Unavailable",
        "Schema Registry circuit breaker is open. The service is temporarily unavailable.",
        cause);
  }
}
