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

package io.confluent.kafkarest.producer;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A producer pool implementation that uses a single shared producer for all topics.
 *
 * <p>This implementation provides backward compatibility with the original single-producer behavior.
 * It wraps a single KafkaProducer instance and returns it for all topic requests.
 */
public class SingleProducerPool implements ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(SingleProducerPool.class);

  private final Producer<byte[], byte[]> producer;

  /**
   * Creates a new SingleProducerPool wrapping the given producer.
   *
   * @param producer the single producer to use for all topics
   */
  public SingleProducerPool(Producer<byte[], byte[]> producer) {
    this.producer = requireNonNull(producer);
    log.info("Single producer pool initialized (pooling disabled)");
  }

  @Override
  public Producer<byte[], byte[]> getProducer(String topicName) {
    return producer;
  }

  @Override
  public int getCurrentSize() {
    return 1;
  }

  @Override
  public int getMaxSize() {
    return 1;
  }

  @Override
  public void shutdown() {
    log.info("Shutting down single producer pool");
    try {
      producer.close(Duration.ofSeconds(30));
      log.info("Single producer pool shutdown complete");
    } catch (Exception e) {
      log.warn("Error closing producer", e);
    }
  }
}
