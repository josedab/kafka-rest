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

package io.confluent.kafkarest.resources.v3;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.entities.v3.HealthCheckData;
import io.confluent.kafkarest.entities.v3.HealthResponse;
import io.confluent.kafkarest.entities.v3.HealthStatus;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.ratelimit.DoNotRateLimit;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Health check endpoints for liveness and readiness probes. */
@Path("/health")
@ResourceName("api.health")
@DoNotRateLimit
public final class HealthResource {

  private static final Logger log = LoggerFactory.getLogger(HealthResource.class);

  private final Provider<KafkaRestContext> kafkaRestContext;
  private final Provider<Optional<SchemaRegistryClient>> schemaRegistryClientProvider;
  private final KafkaRestConfig config;

  @Inject
  public HealthResource(
      Provider<KafkaRestContext> kafkaRestContext,
      Provider<Optional<SchemaRegistryClient>> schemaRegistryClientProvider,
      KafkaRestConfig config) {
    this.kafkaRestContext = requireNonNull(kafkaRestContext);
    this.schemaRegistryClientProvider = requireNonNull(schemaRegistryClientProvider);
    this.config = requireNonNull(config);
  }

  /** Overall health check that verifies all dependencies. */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceName("api.health.get")
  public Response health() {
    Map<String, HealthCheckData> checks = new HashMap<>();
    boolean allHealthy = true;

    // Check Kafka if enabled
    if (config.isHealthKafkaEnabled()) {
      HealthCheckData kafkaCheck = checkKafka();
      checks.put("kafka", kafkaCheck);
      if (kafkaCheck.getStatus() != HealthStatus.UP) {
        allHealthy = false;
      }
    }

    // Check Schema Registry if configured and enabled
    if (config.isHealthSchemaRegistryEnabled() && config.isSchemaRegistryEnabled()) {
      HealthCheckData srCheck = checkSchemaRegistry();
      checks.put("schema_registry", srCheck);
      if (srCheck.getStatus() != HealthStatus.UP) {
        allHealthy = false;
      }
    }

    HealthResponse response =
        HealthResponse.builder()
            .setStatus(allHealthy ? HealthStatus.UP : HealthStatus.DOWN)
            .setChecks(ImmutableMap.copyOf(checks))
            .build();

    int statusCode = allHealthy ? Response.Status.OK.getStatusCode() : 503;
    return Response.status(statusCode).entity(response).build();
  }

  /** Liveness probe - returns 200 if the process is running. */
  @GET
  @Path("/live")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceName("api.health.live")
  public Response liveness() {
    return Response.ok(ImmutableMap.of("status", HealthStatus.UP.getValue())).build();
  }

  /** Readiness probe - returns 200 if the service can serve traffic. */
  @GET
  @Path("/ready")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceName("api.health.ready")
  public Response readiness() {
    try {
      Admin admin = kafkaRestContext.get().getAdmin();
      int timeoutMs = config.getHealthReadinessTimeoutMs();

      // Verify Kafka connectivity by getting cluster ID
      admin.describeCluster().clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);

      return Response.ok(ImmutableMap.of("status", HealthStatus.UP.getValue())).build();
    } catch (Exception e) {
      log.warn("Readiness check failed", e);
      return Response.status(503)
          .entity(
              ImmutableMap.of(
                  "status", HealthStatus.DOWN.getValue(),
                  "reason", e.getMessage() != null ? e.getMessage() : "Unknown error"))
          .build();
    }
  }

  private HealthCheckData checkKafka() {
    try {
      Admin admin = kafkaRestContext.get().getAdmin();
      int timeoutMs = config.getHealthKafkaTimeoutMs();

      String clusterId =
          admin.describeCluster().clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);

      return HealthCheckData.builder()
          .setStatus(HealthStatus.UP)
          .setDetails(ImmutableMap.of("cluster_id", clusterId))
          .build();
    } catch (Exception e) {
      log.warn("Kafka health check failed", e);
      return HealthCheckData.builder()
          .setStatus(HealthStatus.DOWN)
          .setDetails(
              ImmutableMap.of(
                  "error", e.getMessage() != null ? e.getMessage() : "Unknown error"))
          .build();
    }
  }

  private HealthCheckData checkSchemaRegistry() {
    try {
      Optional<SchemaRegistryClient> clientOpt = schemaRegistryClientProvider.get();
      if (clientOpt.isEmpty()) {
        return HealthCheckData.builder()
            .setStatus(HealthStatus.DOWN)
            .setDetails(ImmutableMap.of("error", "Schema Registry client not configured"))
            .build();
      }

      SchemaRegistryClient client = clientOpt.get();
      Collection<String> subjects = client.getAllSubjects();

      return HealthCheckData.builder()
          .setStatus(HealthStatus.UP)
          .setDetails(ImmutableMap.of("subjects_count", subjects.size()))
          .build();
    } catch (Exception e) {
      log.warn("Schema Registry health check failed", e);
      return HealthCheckData.builder()
          .setStatus(HealthStatus.DOWN)
          .setDetails(
              ImmutableMap.of(
                  "error", e.getMessage() != null ? e.getMessage() : "Unknown error"))
          .build();
    }
  }
}
