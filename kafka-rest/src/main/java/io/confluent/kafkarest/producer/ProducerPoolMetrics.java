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

import io.confluent.kafkarest.KafkaRestConfig;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics for tracking producer pool utilization and performance.
 */
public class ProducerPoolMetrics {

  private static final Logger log = LoggerFactory.getLogger(ProducerPoolMetrics.class);

  private static final String GROUP_NAME = "producer-pool-metrics";
  private static final long EXPIRY_SECONDS = TimeUnit.HOURS.toSeconds(1);

  // Sensor names
  private static final String POOL_SIZE_SENSOR_NAME = "pool-size-sensor";
  private static final String POOL_HIT_SENSOR_NAME = "pool-hit-sensor";
  private static final String POOL_MISS_SENSOR_NAME = "pool-miss-sensor";
  private static final String POOL_EVICTION_SENSOR_NAME = "pool-eviction-sensor";

  // Metric names
  static final String POOL_SIZE_METRIC_NAME = "pool-size";
  private static final String POOL_SIZE_METRIC_DOC = "The current number of producers in the pool.";

  static final String POOL_SIZE_MAX_METRIC_NAME = "pool-size-max";
  private static final String POOL_SIZE_MAX_METRIC_DOC =
      "The maximum number of producers in the pool.";

  static final String POOL_HIT_RATE_METRIC_NAME = "pool-hit-rate";
  private static final String POOL_HIT_RATE_METRIC_DOC =
      "The rate of successful producer retrievals from the pool.";

  static final String POOL_HIT_TOTAL_METRIC_NAME = "pool-hit-total";
  private static final String POOL_HIT_TOTAL_METRIC_DOC =
      "The total number of successful producer retrievals from the pool.";

  static final String POOL_MISS_RATE_METRIC_NAME = "pool-miss-rate";
  private static final String POOL_MISS_RATE_METRIC_DOC =
      "The rate of producer retrieval misses (when pool is full).";

  static final String POOL_MISS_TOTAL_METRIC_NAME = "pool-miss-total";
  private static final String POOL_MISS_TOTAL_METRIC_DOC =
      "The total number of producer retrieval misses.";

  static final String POOL_EVICTION_RATE_METRIC_NAME = "pool-eviction-rate";
  private static final String POOL_EVICTION_RATE_METRIC_DOC =
      "The rate of idle producer evictions from the pool.";

  static final String POOL_EVICTION_TOTAL_METRIC_NAME = "pool-eviction-total";
  private static final String POOL_EVICTION_TOTAL_METRIC_DOC =
      "The total number of idle producer evictions.";

  private final Metrics metrics;
  private final String jmxPrefix;
  private final String poolSizeSensorName;
  private final String poolHitSensorName;
  private final String poolMissSensorName;
  private final String poolEvictionSensorName;

  /**
   * Creates a new ProducerPoolMetrics instance.
   *
   * @param config the Kafka REST configuration
   * @param metricsTags additional tags to add to metrics
   */
  public ProducerPoolMetrics(KafkaRestConfig config, Map<String, String> metricsTags) {
    // Tag should only be empty or "tenant" but sort on the key just in case
    SortedMap<String, String> sortedMetricsTags = new TreeMap<>(metricsTags);
    String sensorTags =
        sortedMetricsTags.keySet().stream()
            .map(key -> ":" + metricsTags.get(key))
            .collect(Collectors.joining());

    this.metrics = requireNonNull(config.getMetrics());
    this.jmxPrefix = config.getString(KafkaRestConfig.METRICS_JMX_PREFIX_CONFIG);
    String sensorNamePrefix = jmxPrefix + ":" + GROUP_NAME + ":";
    this.poolSizeSensorName = sensorNamePrefix + POOL_SIZE_SENSOR_NAME + sensorTags;
    this.poolHitSensorName = sensorNamePrefix + POOL_HIT_SENSOR_NAME + sensorTags;
    this.poolMissSensorName = sensorNamePrefix + POOL_MISS_SENSOR_NAME + sensorTags;
    this.poolEvictionSensorName = sensorNamePrefix + POOL_EVICTION_SENSOR_NAME + sensorTags;

    setupSensors(sortedMetricsTags, sensorTags);
  }

  private void setupSensors(Map<String, String> metricsTags, String sensorTags) {
    setupPoolSizeSensor(metricsTags, sensorTags);
    setupPoolHitSensor(metricsTags, sensorTags);
    setupPoolMissSensor(metricsTags, sensorTags);
    setupPoolEvictionSensor(metricsTags, sensorTags);
  }

  private void setupPoolSizeSensor(Map<String, String> metricsTags, String sensorTags) {
    Sensor sensor = createSensor(POOL_SIZE_SENSOR_NAME, sensorTags);
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_SIZE_METRIC_NAME, GROUP_NAME, POOL_SIZE_METRIC_DOC, metricsTags.keySet()),
            metricsTags),
        new Value());
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_SIZE_MAX_METRIC_NAME,
                GROUP_NAME,
                POOL_SIZE_MAX_METRIC_DOC,
                metricsTags.keySet()),
            metricsTags),
        new Max());
  }

  private void setupPoolHitSensor(Map<String, String> metricsTags, String sensorTags) {
    Sensor sensor = createSensor(POOL_HIT_SENSOR_NAME, sensorTags);
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_HIT_RATE_METRIC_NAME,
                GROUP_NAME,
                POOL_HIT_RATE_METRIC_DOC,
                metricsTags.keySet()),
            metricsTags),
        new Rate());
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_HIT_TOTAL_METRIC_NAME,
                GROUP_NAME,
                POOL_HIT_TOTAL_METRIC_DOC,
                metricsTags.keySet()),
            metricsTags),
        new CumulativeSum());
  }

  private void setupPoolMissSensor(Map<String, String> metricsTags, String sensorTags) {
    Sensor sensor = createSensor(POOL_MISS_SENSOR_NAME, sensorTags);
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_MISS_RATE_METRIC_NAME,
                GROUP_NAME,
                POOL_MISS_RATE_METRIC_DOC,
                metricsTags.keySet()),
            metricsTags),
        new Rate());
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_MISS_TOTAL_METRIC_NAME,
                GROUP_NAME,
                POOL_MISS_TOTAL_METRIC_DOC,
                metricsTags.keySet()),
            metricsTags),
        new CumulativeSum());
  }

  private void setupPoolEvictionSensor(Map<String, String> metricsTags, String sensorTags) {
    Sensor sensor = createSensor(POOL_EVICTION_SENSOR_NAME, sensorTags);
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_EVICTION_RATE_METRIC_NAME,
                GROUP_NAME,
                POOL_EVICTION_RATE_METRIC_DOC,
                metricsTags.keySet()),
            metricsTags),
        new Rate());
    sensor.add(
        metrics.metricInstance(
            new MetricNameTemplate(
                POOL_EVICTION_TOTAL_METRIC_NAME,
                GROUP_NAME,
                POOL_EVICTION_TOTAL_METRIC_DOC,
                metricsTags.keySet()),
            metricsTags),
        new CumulativeSum());
  }

  private Sensor createSensor(String name, String sensorTags) {
    String fullSensorName = String.join(":", jmxPrefix, GROUP_NAME, name);
    fullSensorName = fullSensorName.concat(sensorTags);
    return metrics.sensor(fullSensorName, null, EXPIRY_SECONDS);
  }

  /**
   * Records the current pool size.
   *
   * @param size the current pool size
   */
  public void recordPoolSize(int size) {
    recordMetric(poolSizeSensorName, size);
  }

  /**
   * Records a pool hit (successful producer retrieval).
   */
  public void recordPoolHit() {
    recordMetric(poolHitSensorName, 1.0);
  }

  /**
   * Records a pool miss (when pool is full and cannot create new producer).
   */
  public void recordPoolMiss() {
    recordMetric(poolMissSensorName, 1.0);
  }

  /**
   * Records producer evictions.
   *
   * @param count the number of producers evicted
   */
  public void recordEvictions(int count) {
    recordMetric(poolEvictionSensorName, count);
  }

  private void recordMetric(String sensorName, double value) {
    Sensor sensor = metrics.getSensor(sensorName);
    if (sensor != null) {
      sensor.record(value);
    }
  }
}
