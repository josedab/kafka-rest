/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.backends.kafka;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafkarest.DefaultKafkaRestContext;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.producer.DefaultProducerPool;
import io.confluent.kafkarest.producer.ProducerPool;
import io.confluent.kafkarest.producer.ProducerPoolMetrics;
import io.confluent.kafkarest.producer.SingleProducerPool;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A module to configure access to Kafka.
 *
 * <p>Right now this module does little but delegate to {@link KafkaRestContext}, since access to
 * Kafka is currently being configured there. It's the author's intention to move such logic here,
 * and eliminate {@code KafkaRestContext}, once dependence injection is properly used elsewhere.
 */
public final class KafkaModule extends AbstractBinder {

  private static final Logger log = LoggerFactory.getLogger(KafkaModule.class);

  @Override
  protected void configure() {
    bindFactory(KafkaRestContextFactory.class).to(KafkaRestContext.class).in(Singleton.class);

    bindFactory(AdminFactory.class).to(Admin.class).in(Singleton.class);

    bindFactory(ProducerPoolFactory.class).to(ProducerPool.class).in(Singleton.class);

    bindFactory(ProducerFactory.class)
        .to(new TypeLiteral<Producer<byte[], byte[]>>() {})
        .in(Singleton.class);
  }

  private static final class KafkaRestContextFactory implements Factory<KafkaRestContext> {
    private final KafkaRestConfig config;

    @Inject
    private KafkaRestContextFactory(KafkaRestConfig config) {
      this.config = requireNonNull(config);
    }

    @Override
    public KafkaRestContext provide() {
      return new DefaultKafkaRestContext(config);
    }

    @Override
    public void dispose(KafkaRestContext context) {
      context.shutdown();
    }
  }

  private static final class AdminFactory implements Factory<Admin> {
    private final KafkaRestContext context;

    @Inject
    private AdminFactory(KafkaRestContext context) {
      this.context = requireNonNull(context);
    }

    @Override
    public Admin provide() {
      return context.getAdmin();
    }

    @Override
    public void dispose(Admin admin) {
      admin.close();
    }
  }

  private static final class ProducerPoolFactory implements Factory<ProducerPool> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KafkaRestConfig config;
    private final KafkaRestContext context;

    @Inject
    private ProducerPoolFactory(KafkaRestConfig config, KafkaRestContext context) {
      this.config = requireNonNull(config);
      this.context = requireNonNull(context);
    }

    @Override
    public ProducerPool provide() {
      if (config.isProducerPoolEnabled()) {
        log.info("Producer pooling enabled, creating DefaultProducerPool");
        Map<String, Object> producerConfigs = config.getProducerConfigs();
        Map<String, Map<String, Object>> topicConfigs = parseTopicConfigs(
            config.getProducerPoolTopicConfigs());
        int maxPoolSize = config.getProducerPoolMaxSize();
        Duration idleTimeout = Duration.ofMillis(config.getProducerPoolIdleTimeoutMs());

        ProducerPoolMetrics metrics = null;
        if (config.getMetrics() != null) {
          metrics = new ProducerPoolMetrics(config, emptyMap());
        }

        return new DefaultProducerPool(
            producerConfigs, topicConfigs, maxPoolSize, idleTimeout, metrics);
      } else {
        log.info("Producer pooling disabled, using single producer");
        return new SingleProducerPool(context.getProducer());
      }
    }

    private Map<String, Map<String, Object>> parseTopicConfigs(String topicConfigsJson) {
      if (topicConfigsJson == null || topicConfigsJson.trim().isEmpty()) {
        return new HashMap<>();
      }

      try {
        return OBJECT_MAPPER.readValue(
            topicConfigsJson, new TypeReference<Map<String, Map<String, Object>>>() {});
      } catch (Exception e) {
        log.warn("Failed to parse producer.pool.topic.configs, using empty config: {}", e.getMessage());
        return new HashMap<>();
      }
    }

    @Override
    public void dispose(ProducerPool pool) {
      pool.shutdown();
    }
  }

  private static final class ProducerFactory implements Factory<Producer<byte[], byte[]>> {
    private final ProducerPool producerPool;

    @Inject
    private ProducerFactory(ProducerPool producerPool) {
      this.producerPool = requireNonNull(producerPool);
    }

    @Override
    public Producer<byte[], byte[]> provide() {
      // Return a producer for the default topic (empty string)
      // This maintains backward compatibility for code that injects Producer directly
      return producerPool.getProducer("");
    }

    @Override
    public void dispose(Producer<byte[], byte[]> producer) {
      // Producer lifecycle is managed by the pool
      // Don't close individual producers here
    }
  }
}
