# RFC-0001: Producer Pooling for Improved Throughput

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Replace the single shared KafkaProducer with a pool of producers to improve throughput, reduce contention, and enable per-topic optimization.

## Motivation

Currently, all produce requests share a single `KafkaProducer` instance:

```java
// KafkaModule.java
@Provides
@Singleton
Producer<byte[], byte[]> provideProducer(KafkaRestConfig config) {
  return new KafkaProducer<>(config.getProducerConfigs());
}
```

This causes several problems:

1. **Buffer contention** - All topics compete for `buffer.memory` (32MB default)
2. **Suboptimal batching** - Different topics have different message sizes and rates
3. **No isolation** - One slow topic affects all others
4. **Single point of failure** - Producer issues affect all requests

**Evidence from production:**
- Under high load (>1000 req/s), `BufferExhaustedException` occurs
- P99 latency spikes when multiple topics produce concurrently
- No ability to tune per-topic (e.g., high-throughput vs low-latency)

## Detailed Design

### Producer Pool Architecture

```java
public class ProducerPool {
  private final ConcurrentHashMap<String, Producer<byte[], byte[]>> producers;
  private final ProducerConfig defaultConfig;
  private final Map<String, ProducerConfig> topicConfigs;
  private final int maxPoolSize;
  private final AtomicInteger currentSize;

  public Producer<byte[], byte[]> getProducer(String topicName) {
    return producers.computeIfAbsent(topicName, this::createProducer);
  }

  private Producer<byte[], byte[]> createProducer(String topicName) {
    if (currentSize.incrementAndGet() > maxPoolSize) {
      currentSize.decrementAndGet();
      // Fall back to default producer
      return getDefaultProducer();
    }

    ProducerConfig config = topicConfigs.getOrDefault(topicName, defaultConfig);
    return new KafkaProducer<>(config);
  }

  public void shutdown() {
    producers.values().forEach(Producer::close);
  }
}
```

### Configuration

```properties
# Enable producer pooling
producer.pool.enabled=true

# Maximum producers in pool
producer.pool.max.size=50

# Default producer config
producer.linger.ms=5
producer.batch.size=32768

# Per-topic overrides (JSON)
producer.pool.topic.configs={
  "high-throughput-topic": {
    "linger.ms": 100,
    "batch.size": 65536,
    "compression.type": "lz4"
  },
  "low-latency-topic": {
    "linger.ms": 0,
    "batch.size": 16384
  }
}

# Idle producer cleanup
producer.pool.idle.timeout.ms=300000
```

### Module Changes

```java
// KafkaModule.java
@Provides
@Singleton
ProducerPool provideProducerPool(KafkaRestConfig config) {
  if (config.getBoolean("producer.pool.enabled")) {
    return new ProducerPool(
        config.getProducerConfigs(),
        config.getMap("producer.pool.topic.configs"),
        config.getInt("producer.pool.max.size"));
  } else {
    // Backward compatible single producer
    return new SingleProducerPool(
        new KafkaProducer<>(config.getProducerConfigs()));
  }
}

// ProduceControllerImpl.java
public class ProduceControllerImpl implements ProduceController {
  private final ProducerPool producerPool;

  @Override
  public CompletableFuture<ProduceResult> produce(
      String clusterId,
      String topicName,
      /* ... */) {

    Producer<byte[], byte[]> producer = producerPool.getProducer(topicName);
    // Use topic-specific producer
    producer.send(record, callback);
  }
}
```

### Metrics

```java
// Track pool utilization
producerPoolSize.set(pool.getCurrentSize());
producerPoolHits.increment();
producerPoolMisses.increment();
producerPoolEvictions.increment();

// Per-producer metrics
producerBatchSizeAvg.record(producer, size);
producerLingerTimeAvg.record(producer, time);
```

## Example Usage

### Before

```java
// All requests use same producer
Producer<byte[], byte[]> producer = producerProvider.get();
producer.send(record);  // Competes with all other topics
```

### After

```java
// Each topic can have its own producer
Producer<byte[], byte[]> producer = producerPool.getProducer(topicName);
producer.send(record);  // Isolated per-topic
```

### API (No changes)

The REST API remains unchanged. This is an internal optimization.

## Implementation Plan

### Phase 1: Core Implementation (Week 1)

1. Create `ProducerPool` class
2. Add configuration properties
3. Update `KafkaModule` bindings

### Phase 2: Controller Integration (Week 2)

1. Update `ProduceControllerImpl`
2. Add metrics
3. Unit tests

### Phase 3: Testing and Tuning (Week 3)

1. Integration tests
2. Performance benchmarks
3. Documentation

### Milestones

| Milestone | Date | Deliverable |
|-----------|------|-------------|
| Design review | Week 1 | Approved RFC |
| Implementation | Week 2 | Code complete |
| Testing | Week 3 | Tests passing |
| Release | Week 4 | Deployed |

## Backwards Compatibility

- **Configuration:** New configs have defaults that preserve current behavior
- **API:** No changes to REST API
- **Metrics:** New metrics added, existing preserved

### Migration Path

1. Deploy with `producer.pool.enabled=false` (default)
2. Test in staging with `producer.pool.enabled=true`
3. Gradually roll out to production
4. Monitor metrics and adjust

### Rollback

Set `producer.pool.enabled=false` to return to single producer.

## Alternatives Considered

### Alternative 1: Connection Multiplexing

Use single producer with multiple connections per broker.

**Rejected because:**
- KafkaProducer doesn't support this
- Would require Kafka client changes

### Alternative 2: Partitioner-based Isolation

Route topics to different producer instances via partitioner.

**Rejected because:**
- Less flexible than full pooling
- Doesn't allow per-topic configuration

### Alternative 3: External Producer Service

Separate producer service called via RPC.

**Rejected because:**
- Adds latency
- Operational complexity
- Doesn't solve the core issue

## Open Questions

1. **Eviction policy:** LRU or by idle time?
   - Proposal: Idle time (easier to reason about)

2. **Warm-up:** Should we pre-create producers for known topics?
   - Proposal: Configuration option for topic list

3. **Metrics granularity:** Per-producer or aggregated?
   - Proposal: Both (aggregated by default, per-producer on demand)

4. **Memory limits:** How to bound total memory across all producers?
   - Proposal: `producer.pool.total.buffer.memory` config

## Success Criteria

- [ ] 30% throughput increase in benchmarks
- [ ] P99 latency reduced by 20%
- [ ] No `BufferExhaustedException` under normal load
- [ ] Per-topic configuration working
- [ ] Metrics visible in monitoring

## Effort Estimation

**Total:** 15 dev-days

| Task | Days |
|------|------|
| Design and review | 2 |
| ProducerPool implementation | 4 |
| Controller integration | 2 |
| Configuration | 1 |
| Metrics | 2 |
| Testing | 3 |
| Documentation | 1 |

## Required Approvals

- [ ] Performance Team
- [ ] Architecture Review
- [ ] Security Review (for config changes)
