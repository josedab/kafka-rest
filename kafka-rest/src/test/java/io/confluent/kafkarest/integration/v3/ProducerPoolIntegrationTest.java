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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for producer pooling functionality.
 */
public class ProducerPoolIntegrationTest extends ClusterTestHarness {

  private static final String TOPIC_1 = "producer-pool-test-topic-1";
  private static final String TOPIC_2 = "producer-pool-test-topic-2";

  public ProducerPoolIntegrationTest() {
    super(/* numBrokers= */ 1, /* withSchemaRegistry= */ false);
  }

  @Override
  protected void overrideKafkaRestConfigs(Properties restProperties) {
    // Enable producer pooling
    restProperties.put(KafkaRestConfig.PRODUCER_POOL_ENABLED_CONFIG, "true");
    restProperties.put(KafkaRestConfig.PRODUCER_POOL_MAX_SIZE_CONFIG, "10");
    restProperties.put(KafkaRestConfig.PRODUCER_POOL_IDLE_TIMEOUT_MS_CONFIG, "300000");
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTopic(TOPIC_1, 3, (short) 1);
    createTopic(TOPIC_2, 3, (short) 1);
  }

  @Test
  public void testProduceWithPoolingEnabled_succeeds() {
    String clusterId = getClusterId();

    // Produce to first topic
    Response response1 = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/records")
        .accept(MediaType.APPLICATION_JSON)
        .post(Entity.entity(
            "{\"value\": {\"type\": \"BINARY\", \"data\": \"dGVzdC12YWx1ZS0x\"}}",
            MediaType.APPLICATION_JSON));

    assertEquals(Status.OK.getStatusCode(), response1.getStatus());

    // Produce to second topic
    Response response2 = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_2 + "/records")
        .accept(MediaType.APPLICATION_JSON)
        .post(Entity.entity(
            "{\"value\": {\"type\": \"BINARY\", \"data\": \"dGVzdC12YWx1ZS0y\"}}",
            MediaType.APPLICATION_JSON));

    assertEquals(Status.OK.getStatusCode(), response2.getStatus());
  }

  @Test
  public void testMultipleProducesToSameTopic_succeeds() {
    String clusterId = getClusterId();

    // Produce multiple messages to the same topic
    for (int i = 0; i < 5; i++) {
      Response response = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/records")
          .accept(MediaType.APPLICATION_JSON)
          .post(Entity.entity(
              "{\"value\": {\"type\": \"BINARY\", \"data\": \"dGVzdC12YWx1ZQ==\"}}",
              MediaType.APPLICATION_JSON));

      assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @Test
  public void testProduceWithKeyAndValue_succeeds() {
    String clusterId = getClusterId();

    Response response = request("/v3/clusters/" + clusterId + "/topics/" + TOPIC_1 + "/records")
        .accept(MediaType.APPLICATION_JSON)
        .post(Entity.entity(
            "{\"key\": {\"type\": \"BINARY\", \"data\": \"a2V5LTE=\"}, "
                + "\"value\": {\"type\": \"BINARY\", \"data\": \"dmFsdWUtMQ==\"}}",
            MediaType.APPLICATION_JSON));

    assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
}
