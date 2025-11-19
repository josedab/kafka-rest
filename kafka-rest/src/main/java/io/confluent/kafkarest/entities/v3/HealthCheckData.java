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

/** Health check result for a single component. */
@AutoValue
public abstract class HealthCheckData {

  HealthCheckData() {}

  @JsonProperty("status")
  public abstract HealthStatus getStatus();

  @JsonProperty("details")
  public abstract ImmutableMap<String, Object> getDetails();

  public static Builder builder() {
    return new AutoValue_HealthCheckData.Builder().setDetails(ImmutableMap.of());
  }

  @JsonCreator
  static HealthCheckData fromJson(
      @JsonProperty("status") HealthStatus status,
      @JsonProperty("details") Map<String, Object> details) {
    return builder()
        .setStatus(status)
        .setDetails(details != null ? ImmutableMap.copyOf(details) : ImmutableMap.of())
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStatus(HealthStatus status);

    public abstract Builder setDetails(ImmutableMap<String, Object> details);

    public Builder setDetails(Map<String, Object> details) {
      return setDetails(ImmutableMap.copyOf(details));
    }

    public abstract HealthCheckData build();
  }
}
