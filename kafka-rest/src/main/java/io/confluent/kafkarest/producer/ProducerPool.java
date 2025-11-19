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

import org.apache.kafka.clients.producer.Producer;

/**
 * Interface for managing a pool of Kafka producers.
 *
 * <p>This interface allows for different implementations of producer pooling, including
 * topic-specific producers and a single shared producer for backward compatibility.
 */
public interface ProducerPool {

  /**
   * Gets a producer for the specified topic.
   *
   * @param topicName the name of the topic
   * @return a producer instance suitable for the topic
   */
  Producer<byte[], byte[]> getProducer(String topicName);

  /**
   * Gets the current number of producers in the pool.
   *
   * @return the current pool size
   */
  int getCurrentSize();

  /**
   * Gets the maximum allowed pool size.
   *
   * @return the maximum pool size
   */
  int getMaxSize();

  /**
   * Shuts down the producer pool, closing all producers.
   */
  void shutdown();
}
