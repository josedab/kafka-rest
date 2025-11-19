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

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.entities.v3.HealthCheckData;
import io.confluent.kafkarest.entities.v3.HealthResponse;
import io.confluent.kafkarest.entities.v3.HealthStatus;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class HealthResourceTest {

  private static final String CLUSTER_ID = "test-cluster-123";

  @Mock private KafkaRestContext kafkaRestContext;
  @Mock private Admin admin;
  @Mock private DescribeClusterResult describeClusterResult;
  @Mock private KafkaFuture<String> clusterIdFuture;
  @Mock private SchemaRegistryClient schemaRegistryClient;
  @Mock private KafkaRestConfig config;

  private HealthResource healthResource;

  @BeforeEach
  public void setUp() {
    healthResource =
        new HealthResource(
            () -> kafkaRestContext, () -> Optional.of(schemaRegistryClient), config);
  }

  @Test
  public void liveness_returnsUp() {
    Response response = healthResource.liveness();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, String> entity = (Map<String, String>) response.getEntity();
    assertEquals("UP", entity.get("status"));
  }

  @Test
  public void readiness_kafkaHealthy_returnsUp() throws Exception {
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class))).andReturn(CLUSTER_ID);
    expect(config.getHealthReadinessTimeoutMs()).andReturn(5000);
    replay(kafkaRestContext, admin, describeClusterResult, clusterIdFuture, config);

    Response response = healthResource.readiness();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, String> entity = (Map<String, String>) response.getEntity();
    assertEquals("UP", entity.get("status"));
  }

  @Test
  public void readiness_kafkaUnhealthy_returnsDown() throws Exception {
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class)))
        .andThrow(new ExecutionException(new RuntimeException("Connection refused")));
    expect(config.getHealthReadinessTimeoutMs()).andReturn(5000);
    replay(kafkaRestContext, admin, describeClusterResult, clusterIdFuture, config);

    Response response = healthResource.readiness();

    assertEquals(503, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, String> entity = (Map<String, String>) response.getEntity();
    assertEquals("DOWN", entity.get("status"));
    assertNotNull(entity.get("reason"));
  }

  @Test
  public void readiness_kafkaTimeout_returnsDown() throws Exception {
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class)))
        .andThrow(new TimeoutException("Timed out waiting for cluster metadata"));
    expect(config.getHealthReadinessTimeoutMs()).andReturn(5000);
    replay(kafkaRestContext, admin, describeClusterResult, clusterIdFuture, config);

    Response response = healthResource.readiness();

    assertEquals(503, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, String> entity = (Map<String, String>) response.getEntity();
    assertEquals("DOWN", entity.get("status"));
  }

  @Test
  public void health_allHealthy_returnsUp() throws Exception {
    expect(config.isHealthKafkaEnabled()).andReturn(true);
    expect(config.isHealthSchemaRegistryEnabled()).andReturn(true);
    expect(config.isSchemaRegistryEnabled()).andReturn(true);
    expect(config.getHealthKafkaTimeoutMs()).andReturn(5000);

    // Kafka check
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class))).andReturn(CLUSTER_ID);

    // Schema Registry check
    expect(schemaRegistryClient.getAllSubjects()).andReturn(Arrays.asList("subject1", "subject2"));

    replay(
        kafkaRestContext,
        admin,
        describeClusterResult,
        clusterIdFuture,
        schemaRegistryClient,
        config);

    Response response = healthResource.health();

    assertEquals(200, response.getStatus());
    HealthResponse healthResponse = (HealthResponse) response.getEntity();
    assertEquals(HealthStatus.UP, healthResponse.getStatus());
    assertEquals(HealthStatus.UP, healthResponse.getChecks().get("kafka").getStatus());
    assertEquals(
        CLUSTER_ID, healthResponse.getChecks().get("kafka").getDetails().get("cluster_id"));
    assertEquals(HealthStatus.UP, healthResponse.getChecks().get("schema_registry").getStatus());
    assertEquals(
        2, healthResponse.getChecks().get("schema_registry").getDetails().get("subjects_count"));
  }

  @Test
  public void health_kafkaUnhealthy_returnsDown() throws Exception {
    expect(config.isHealthKafkaEnabled()).andReturn(true);
    expect(config.isHealthSchemaRegistryEnabled()).andReturn(false);
    expect(config.getHealthKafkaTimeoutMs()).andReturn(5000);

    // Kafka check fails
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class)))
        .andThrow(new ExecutionException(new RuntimeException("Connection refused")));

    replay(kafkaRestContext, admin, describeClusterResult, clusterIdFuture, config);

    Response response = healthResource.health();

    assertEquals(503, response.getStatus());
    HealthResponse healthResponse = (HealthResponse) response.getEntity();
    assertEquals(HealthStatus.DOWN, healthResponse.getStatus());
    assertEquals(HealthStatus.DOWN, healthResponse.getChecks().get("kafka").getStatus());
  }

  @Test
  public void health_schemaRegistryUnhealthy_returnsDown() throws Exception {
    expect(config.isHealthKafkaEnabled()).andReturn(true);
    expect(config.isHealthSchemaRegistryEnabled()).andReturn(true);
    expect(config.isSchemaRegistryEnabled()).andReturn(true);
    expect(config.getHealthKafkaTimeoutMs()).andReturn(5000);

    // Kafka check succeeds
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class))).andReturn(CLUSTER_ID);

    // Schema Registry check fails
    expect(schemaRegistryClient.getAllSubjects())
        .andThrow(new RuntimeException("Schema Registry unavailable"));

    replay(
        kafkaRestContext,
        admin,
        describeClusterResult,
        clusterIdFuture,
        schemaRegistryClient,
        config);

    Response response = healthResource.health();

    assertEquals(503, response.getStatus());
    HealthResponse healthResponse = (HealthResponse) response.getEntity();
    assertEquals(HealthStatus.DOWN, healthResponse.getStatus());
    assertEquals(HealthStatus.UP, healthResponse.getChecks().get("kafka").getStatus());
    assertEquals(HealthStatus.DOWN, healthResponse.getChecks().get("schema_registry").getStatus());
  }

  @Test
  public void health_kafkaDisabled_skipsKafkaCheck() throws Exception {
    expect(config.isHealthKafkaEnabled()).andReturn(false);
    expect(config.isHealthSchemaRegistryEnabled()).andReturn(false);

    replay(config);

    Response response = healthResource.health();

    assertEquals(200, response.getStatus());
    HealthResponse healthResponse = (HealthResponse) response.getEntity();
    assertEquals(HealthStatus.UP, healthResponse.getStatus());
    assertEquals(0, healthResponse.getChecks().size());
  }

  @Test
  public void health_schemaRegistryDisabledInConfig_skipsSchemaRegistryCheck() throws Exception {
    expect(config.isHealthKafkaEnabled()).andReturn(true);
    expect(config.isHealthSchemaRegistryEnabled()).andReturn(true);
    expect(config.isSchemaRegistryEnabled()).andReturn(false);
    expect(config.getHealthKafkaTimeoutMs()).andReturn(5000);

    // Kafka check succeeds
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class))).andReturn(CLUSTER_ID);

    replay(kafkaRestContext, admin, describeClusterResult, clusterIdFuture, config);

    Response response = healthResource.health();

    assertEquals(200, response.getStatus());
    HealthResponse healthResponse = (HealthResponse) response.getEntity();
    assertEquals(HealthStatus.UP, healthResponse.getStatus());
    assertEquals(1, healthResponse.getChecks().size());
    assertEquals(HealthStatus.UP, healthResponse.getChecks().get("kafka").getStatus());
  }

  @Test
  public void health_schemaRegistryClientNotConfigured_returnsDown() throws Exception {
    // Create health resource without schema registry client
    healthResource =
        new HealthResource(() -> kafkaRestContext, () -> Optional.empty(), config);

    expect(config.isHealthKafkaEnabled()).andReturn(true);
    expect(config.isHealthSchemaRegistryEnabled()).andReturn(true);
    expect(config.isSchemaRegistryEnabled()).andReturn(true);
    expect(config.getHealthKafkaTimeoutMs()).andReturn(5000);

    // Kafka check succeeds
    expect(kafkaRestContext.getAdmin()).andReturn(admin);
    expect(admin.describeCluster()).andReturn(describeClusterResult);
    expect(describeClusterResult.clusterId()).andReturn(clusterIdFuture);
    expect(clusterIdFuture.get(anyInt(), anyObject(TimeUnit.class))).andReturn(CLUSTER_ID);

    replay(kafkaRestContext, admin, describeClusterResult, clusterIdFuture, config);

    Response response = healthResource.health();

    assertEquals(503, response.getStatus());
    HealthResponse healthResponse = (HealthResponse) response.getEntity();
    assertEquals(HealthStatus.DOWN, healthResponse.getStatus());
    assertEquals(HealthStatus.UP, healthResponse.getChecks().get("kafka").getStatus());
    assertEquals(HealthStatus.DOWN, healthResponse.getChecks().get("schema_registry").getStatus());
  }
}
