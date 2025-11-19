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

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Response for health check endpoints. */
@AutoValue
public abstract class HealthResponse {

  HealthResponse() {}

  @JsonProperty("status")
  public abstract HealthStatus getStatus();

  @JsonProperty("checks")
  public abstract ImmutableMap<String, HealthCheckData> getChecks();

  public static Builder builder() {
    return new AutoValue_HealthResponse.Builder().setChecks(ImmutableMap.of());
  }

  @JsonCreator
  static HealthResponse fromJson(
      @JsonProperty("status") HealthStatus status,
      @JsonProperty("checks") Map<String, HealthCheckData> checks) {
    return builder()
        .setStatus(status)
        .setChecks(checks != null ? ImmutableMap.copyOf(checks) : ImmutableMap.of())
        .build();
  }

  public HealthResponse withStatus(HealthStatus status) {
    return builder().setStatus(status).setChecks(getChecks()).build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStatus(HealthStatus status);

    public abstract Builder setChecks(ImmutableMap<String, HealthCheckData> checks);

    public Builder setChecks(Map<String, HealthCheckData> checks) {
      return setChecks(ImmutableMap.copyOf(checks));
    }

    public abstract HealthResponse build();
  }
}
