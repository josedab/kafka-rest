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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;

public class SingleProducerPoolTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testGetProducer_alwaysReturnsSameProducer() {
    Producer<byte[], byte[]> mockProducer = mock(Producer.class);
    SingleProducerPool pool = new SingleProducerPool(mockProducer);

    Producer<byte[], byte[]> producer1 = pool.getProducer("topic-1");
    Producer<byte[], byte[]> producer2 = pool.getProducer("topic-2");
    Producer<byte[], byte[]> producer3 = pool.getProducer("any-topic");

    assertSame(mockProducer, producer1);
    assertSame(mockProducer, producer2);
    assertSame(mockProducer, producer3);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetCurrentSize_alwaysReturnsOne() {
    Producer<byte[], byte[]> mockProducer = mock(Producer.class);
    SingleProducerPool pool = new SingleProducerPool(mockProducer);

    assertEquals(1, pool.getCurrentSize());

    // Getting producers for multiple topics shouldn't change size
    pool.getProducer("topic-1");
    pool.getProducer("topic-2");

    assertEquals(1, pool.getCurrentSize());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetMaxSize_alwaysReturnsOne() {
    Producer<byte[], byte[]> mockProducer = mock(Producer.class);
    SingleProducerPool pool = new SingleProducerPool(mockProducer);

    assertEquals(1, pool.getMaxSize());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testShutdown_closesProducer() {
    Producer<byte[], byte[]> mockProducer = mock(Producer.class);
    SingleProducerPool pool = new SingleProducerPool(mockProducer);

    pool.shutdown();

    verify(mockProducer).close(org.mockito.ArgumentMatchers.any());
  }
}
