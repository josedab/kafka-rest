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

package io.confluent.kafkarest.integration.v3;

import static io.confluent.kafkarest.TestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafkarest.entities.v3.HealthResponse;
import io.confluent.kafkarest.entities.v3.HealthStatus;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class HealthResourceIntegrationTest extends ClusterTestHarness {

  public HealthResourceIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void liveness_returnsUp(String quorum) {
    Response response = request("/health/live").accept(MediaType.APPLICATION_JSON).get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    @SuppressWarnings("unchecked")
    Map<String, String> body = response.readEntity(Map.class);
    assertEquals("UP", body.get("status"));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void readiness_clusterHealthy_returnsUp(String quorum) {
    Response response = request("/health/ready").accept(MediaType.APPLICATION_JSON).get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    @SuppressWarnings("unchecked")
    Map<String, String> body = response.readEntity(Map.class);
    assertEquals("UP", body.get("status"));
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void health_clusterHealthy_returnsUp(String quorum) {
    Response response = request("/health").accept(MediaType.APPLICATION_JSON).get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());

    HealthResponse healthResponse = response.readEntity(HealthResponse.class);
    assertEquals(HealthStatus.UP, healthResponse.getStatus());

    // Kafka check should be present and UP
    assertNotNull(healthResponse.getChecks().get("kafka"));
    assertEquals(HealthStatus.UP, healthResponse.getChecks().get("kafka").getStatus());

    // Verify cluster_id is in the details
    Object clusterId = healthResponse.getChecks().get("kafka").getDetails().get("cluster_id");
    assertNotNull(clusterId);
    assertTrue(clusterId.toString().length() > 0);
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void health_returnsCorrectContentType(String quorum) {
    Response response = request("/health").accept(MediaType.APPLICATION_JSON).get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertTrue(
        response.getMediaType().toString().contains(MediaType.APPLICATION_JSON),
        "Expected application/json content type");
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void liveness_returnsCorrectContentType(String quorum) {
    Response response = request("/health/live").accept(MediaType.APPLICATION_JSON).get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertTrue(
        response.getMediaType().toString().contains(MediaType.APPLICATION_JSON),
        "Expected application/json content type");
  }

  @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
  @ValueSource(strings = {"kraft"})
  public void readiness_returnsCorrectContentType(String quorum) {
    Response response = request("/health/ready").accept(MediaType.APPLICATION_JSON).get();

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    assertTrue(
        response.getMediaType().toString().contains(MediaType.APPLICATION_JSON),
        "Expected application/json content type");
  }
}
