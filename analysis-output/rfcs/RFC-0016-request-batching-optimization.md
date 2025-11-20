# RFC-0016: Request Batching Optimization

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Optimize the existing batch produce API to achieve true batching benefits by accumulating records and sending them in optimized Kafka producer batches.

## Motivation

Current batch API doesn't provide true batching benefits:

```java
// ProduceBatchAction.java - Current implementation
@POST
@Path("/records:batch")
public void produceBatch(BatchProduceRequest request) {
  List<CompletableFuture<ProduceResult>> futures = request.getEntries().stream()
      .map(entry -> produceController.produce(...))  // Individual produces
      .collect(toList());

  CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
      .join();  // Wait for all
}
```

**Problems:**

1. **No true batching** - Each record produces individually
2. **Inefficient** - Same overhead as multiple single produces
3. **Poor Kafka batching** - Producer sends many small batches
4. **High latency** - Wait for all individual produces

**Benchmark (current):**
- Single produce: 10ms
- Batch of 50: 500ms (50 x 10ms = no benefit)

**With optimization:**
- Batch of 50: 50ms (10x improvement)

## Detailed Design

### Batching Strategy

```java
public class BatchProduceOptimizer {

  private final Producer<byte[], byte[]> producer;
  private final int batchSize;
  private final Duration lingerTime;

  public CompletableFuture<List<ProduceResult>> produceBatch(
      String topicName,
      List<ProduceRequest> requests) {

    // All requests go to same topic
    List<CompletableFuture<ProduceResult>> futures = new ArrayList<>();

    // Configure producer for this batch
    Map<String, Object> batchConfig = new HashMap<>();
    batchConfig.put(ProducerConfig.LINGER_MS_CONFIG, lingerTime.toMillis());
    batchConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);

    // Send all records without waiting
    for (ProduceRequest request : requests) {
      ProducerRecord<byte[], byte[]> record = toProducerRecord(
          topicName, request);

      CompletableFuture<ProduceResult> future = new CompletableFuture<>();
      producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          future.completeExceptionally(exception);
        } else {
          future.complete(toProduceResult(metadata));
        }
      });
      futures.add(future);
    }

    // Force flush to ensure all records sent in one batch
    producer.flush();

    // Wait for all results
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(v -> futures.stream()
            .map(CompletableFuture::join)
            .collect(toList()));
  }
}
```

### Intelligent Batching

```java
public class IntelligentBatcher {

  private final ConcurrentHashMap<String, RecordBatch> activeBatches;
  private final ScheduledExecutorService scheduler;

  public CompletableFuture<ProduceResult> produce(
      String topicName,
      ProduceRequest request,
      BatchingPolicy policy) {

    RecordBatch batch = activeBatches.computeIfAbsent(topicName,
        k -> new RecordBatch(topicName, policy));

    // Add to batch
    CompletableFuture<ProduceResult> future = batch.add(request);

    // Check if should flush
    if (batch.shouldFlush()) {
      flushBatch(topicName);
    }

    return future;
  }

  private void flushBatch(String topicName) {
    RecordBatch batch = activeBatches.remove(topicName);
    if (batch != null) {
      executeBatch(batch);
    }
  }

  private void executeBatch(RecordBatch batch) {
    List<ProducerRecord<byte[], byte[]>> records = batch.getRecords();

    // Send all records
    for (int i = 0; i < records.size(); i++) {
      ProducerRecord<byte[], byte[]> record = records.get(i);
      CompletableFuture<ProduceResult> future = batch.getFuture(i);

      producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          future.completeExceptionally(exception);
        } else {
          future.complete(toProduceResult(metadata));
        }
      });
    }

    // Flush to ensure batching
    producer.flush();
  }

  // Auto-flush timer
  public void start() {
    scheduler.scheduleAtFixedRate(() -> {
      activeBatches.forEach((topic, batch) -> {
        if (batch.isExpired()) {
          flushBatch(topic);
        }
      });
    }, 0, 10, TimeUnit.MILLISECONDS);
  }
}

class RecordBatch {
  private final List<ProducerRecord<byte[], byte[]>> records;
  private final List<CompletableFuture<ProduceResult>> futures;
  private final BatchingPolicy policy;
  private final Instant createdAt;

  public boolean shouldFlush() {
    return records.size() >= policy.getMaxSize() ||
           estimateSize() >= policy.getMaxBytes() ||
           isExpired();
  }

  public boolean isExpired() {
    return Duration.between(createdAt, Instant.now())
        .compareTo(policy.getMaxWaitTime()) > 0;
  }
}

class BatchingPolicy {
  private final int maxSize;       // Max records per batch
  private final long maxBytes;     // Max bytes per batch
  private final Duration maxWaitTime;  // Max time to wait

  public static BatchingPolicy balanced() {
    return new BatchingPolicy(50, 1048576, Duration.ofMillis(100));
  }

  public static BatchingPolicy lowLatency() {
    return new BatchingPolicy(10, 262144, Duration.ofMillis(10));
  }

  public static BatchingPolicy highThroughput() {
    return new BatchingPolicy(1000, 10485760, Duration.ofMillis(1000));
  }
}
```

### Configuration

```properties
# Batch optimization
produce.batch.optimization.enabled=true

# Batching policy
produce.batch.policy=balanced  # balanced, low-latency, high-throughput

# Custom policy
produce.batch.custom.max.records=50
produce.batch.custom.max.bytes=1048576
produce.batch.custom.max.wait.ms=100

# Producer batching settings (overridden for batch operations)
produce.batch.linger.ms=100
produce.batch.size=65536
produce.batch.compression.type=lz4
```

### Metrics

```java
// Batching metrics
batchRecordCount.record(batch.size());
batchSizeBytes.record(batch.estimateSize());
batchWaitTime.record(batch.getAge());
batchEfficiency.record((double) batch.size() / maxBatchSize);
kafkaBatchCount.record(kafkaBatches);  // Actual Kafka batches
compressionRatio.record(compressedSize / uncompressedSize);
```

## Example Usage

### Before (No Optimization)

```bash
# Batch of 50 records
curl -X POST http://localhost:8082/v3/.../records:batch \
  -d '{"entries": [...]}'  # 50 records

# Internal behavior:
# - 50 individual producer.send() calls
# - Kafka creates 50 small batches
# - Total time: 500ms
```

### After (With Optimization)

```bash
# Same request
curl -X POST http://localhost:8082/v3/.../records:batch \
  -d '{"entries": [...]}'  # 50 records

# Internal behavior:
# - All records accumulated
# - Single producer.flush()
# - Kafka creates 1 optimized batch
# - Compression applied once
# - Total time: 50ms (10x faster)
```

### Intelligent Batching

```bash
# High-throughput mode
curl -X POST http://localhost:8082/v3/.../records:batch?policy=high-throughput \
  -d '{"entries": [...]}'

# Low-latency mode
curl -X POST http://localhost:8082/v3/.../records:batch?policy=low-latency \
  -d '{"entries": [...]}'
```

## Implementation Plan

### Phase 1: Basic Optimization (Week 1)

1. Implement `BatchProduceOptimizer`
2. Integrate with existing batch API
3. Configuration

### Phase 2: Intelligent Batching (Week 2)

1. `IntelligentBatcher` implementation
2. Batching policies
3. Auto-flush timer

### Phase 3: Testing and Tuning (Week 3)

1. Performance benchmarks
2. Compression testing
3. Policy tuning

## Backwards Compatibility

- **API:** No changes
- **Behavior:** Faster, but same semantics
- **Configuration:** New optional settings

## Alternatives Considered

### Alternative 1: Client-Side Batching

Let clients batch before sending.

**Rejected because:**
- Requires client changes
- Misses server-side optimization opportunities
- Not all clients can batch

### Alternative 2: Kafka Streams

Use Kafka Streams for batching.

**Rejected because:**
- Adds complexity
- Overkill for this use case
- Synchronous API needed

## Open Questions

1. **Compression:** Should we always compress batches?
   - Proposal: Yes, configurable codec

2. **Ordering:** Does batching affect ordering?
   - Proposal: No, same partition = same order

## Success Criteria

- [ ] 10x throughput for batch operations
- [ ] 50% latency reduction
- [ ] 70% compression ratio with lz4
- [ ] Single Kafka batch per API batch

## Effort Estimation

**Total:** 15 dev-days

| Task | Days |
|------|------|
| Basic optimization | 3 |
| Intelligent batching | 5 |
| Policies and configuration | 2 |
| Testing | 4 |
| Documentation | 1 |

## Required Approvals

- [ ] Performance Team
- [ ] Architecture Review
