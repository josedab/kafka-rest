/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafkarest.v2;

import io.confluent.kafkarest.ConsumerInstanceId;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks the state of an async consumer instance for non-blocking operations.
 * This class wraps a KafkaConsumerState and provides additional tracking for
 * async polling operations.
 *
 * @param <KafkaKeyT> The type of the key as returned by the Kafka consumer
 * @param <KafkaValueT> The type of the value as returned by the Kafka consumer
 * @param <ClientKeyT> The type of the key to return to the client
 * @param <ClientValueT> The type of the value to return to the client
 */
public class AsyncConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> {

  private final KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> consumerState;
  private final ConsumerInstanceId instanceId;
  private final Clock clock;
  private final Duration instanceTimeout;

  private volatile Instant expiration;
  private final Object expirationLock = new Object();

  // Track pending polls for backpressure management
  private final AtomicInteger pendingPolls = new AtomicInteger(0);

  public AsyncConsumerState(
      KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> consumerState,
      ConsumerInstanceId instanceId,
      Duration instanceTimeout) {
    this(consumerState, instanceId, instanceTimeout, Clock.systemUTC());
  }

  // Package-private constructor for testing with custom clock
  AsyncConsumerState(
      KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> consumerState,
      ConsumerInstanceId instanceId,
      Duration instanceTimeout,
      Clock clock) {
    this.consumerState = consumerState;
    this.instanceId = instanceId;
    this.clock = clock;
    this.instanceTimeout = instanceTimeout;
    this.expiration = clock.instant().plus(instanceTimeout);
  }

  /**
   * Returns the underlying KafkaConsumerState.
   *
   * @return the consumer state
   */
  public KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> getConsumerState() {
    return consumerState;
  }

  /**
   * Returns the consumer instance ID.
   *
   * @return the instance ID
   */
  public ConsumerInstanceId getInstanceId() {
    return instanceId;
  }

  /**
   * Checks if the consumer instance has expired.
   *
   * @param now the current instant
   * @return true if expired, false otherwise
   */
  public boolean expired(Instant now) {
    synchronized (expirationLock) {
      return !expiration.isAfter(now);
    }
  }

  /**
   * Updates the expiration time to now + instance timeout.
   */
  public void updateExpiration() {
    synchronized (expirationLock) {
      this.expiration = clock.instant().plus(instanceTimeout);
    }
    // Also update the underlying consumer state expiration
    consumerState.updateExpiration();
  }

  /**
   * Increments the pending poll count.
   *
   * @return the new pending poll count
   */
  public int incrementPendingPolls() {
    return pendingPolls.incrementAndGet();
  }

  /**
   * Decrements the pending poll count.
   *
   * @return the new pending poll count
   */
  public int decrementPendingPolls() {
    return pendingPolls.decrementAndGet();
  }

  /**
   * Returns the current pending poll count.
   *
   * @return the pending poll count
   */
  public int getPendingPolls() {
    return pendingPolls.get();
  }

  /**
   * Closes the async consumer state and the underlying consumer.
   */
  public void close() {
    consumerState.close();
  }
}
