# RFC-0015: Admin Operations Enhancement

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Enhance admin operations with bulk APIs, async processing, and better error handling to improve efficiency for large-scale Kafka cluster management.

## Motivation

Current admin operations are inefficient for bulk operations:

```java
// Current: Create topics one-by-one
POST /v3/clusters/{id}/topics {"topic_name": "topic1"}
POST /v3/clusters/{id}/topics {"topic_name": "topic2"}
POST /v3/clusters/{id}/topics {"topic_name": "topic3"}
// 3 requests, 3 round-trips
```

**Problems:**

1. **Inefficient bulk operations** - 100 topics = 100 HTTP requests
2. **No parallel execution** - Operations processed sequentially
3. **Poor error handling** - One failure stops all operations
4. **No progress tracking** - Long operations block with no status

**Real-world scenario:**
- Need to create 1000 topics
- Current: 1000 HTTP requests, 10+ minutes
- **With bulk API: 1 request, < 1 minute**

## Detailed Design

### Bulk Topic Creation

```java
// BulkTopicCreateAction.java
@POST
@Path("/:batch")
@Consumes(MediaType.APPLICATION_JSON)
public void createTopicsBatch(
    @Suspended AsyncResponse asyncResponse,
    @PathParam("clusterId") String clusterId,
    BulkTopicCreateRequest request) {

  List<CompletableFuture<TopicCreateResult>> futures =
      request.getTopics().stream()
          .map(topicSpec -> createTopicAsync(clusterId, topicSpec))
          .collect(toList());

  CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
      .thenApply(v -> futures.stream()
          .map(CompletableFuture::join)
          .collect(toList()))
      .thenApply(results -> BulkTopicCreateResponse.builder()
          .setSuccesses(results.stream()
              .filter(r -> r.getError() == null)
              .collect(toList()))
          .setFailures(results.stream()
              .filter(r -> r.getError() != null)
              .collect(toList()))
          .build())
      .whenComplete((response, error) -> {
        if (error != null) {
          asyncResponse.resume(error);
        } else {
          asyncResponse.resume(Response.ok(response).build());
        }
      });
}

private CompletableFuture<TopicCreateResult> createTopicAsync(
    String clusterId,
    TopicSpec topicSpec) {

  return topicManager.createTopic(clusterId, topicSpec)
      .thenApply(topic -> TopicCreateResult.success(topic))
      .exceptionally(error -> TopicCreateResult.failure(
          topicSpec.getTopicName(),
          error.getMessage()));
}
```

### Request/Response Models

```java
// BulkTopicCreateRequest.java
public abstract class BulkTopicCreateRequest {

  @JsonProperty("topics")
  @Size(min = 1, max = 1000, message = "Batch size must be 1-1000")
  public abstract List<TopicSpec> getTopics();

  @JsonProperty("validate_only")
  public abstract Optional<Boolean> getValidateOnly();
}

// TopicSpec.java
public abstract class TopicSpec {
  @JsonProperty("topic_name")
  @ValidTopicName
  public abstract String getTopicName();

  @JsonProperty("partitions_count")
  @Min(1)
  public abstract Optional<Integer> getPartitionsCount();

  @JsonProperty("replication_factor")
  @Min(1)
  public abstract Optional<Integer> getReplicationFactor();

  @JsonProperty("configs")
  public abstract Optional<Map<String, String>> getConfigs();
}

// BulkTopicCreateResponse.java
public abstract class BulkTopicCreateResponse {

  @JsonProperty("successes")
  public abstract List<TopicData> getSuccesses();

  @JsonProperty("failures")
  public abstract List<OperationFailure> getFailures();

  @JsonProperty("summary")
  public OperationSummary getSummary() {
    return OperationSummary.builder()
        .setTotal(getSuccesses().size() + getFailures().size())
        .setSuccessCount(getSuccesses().size())
        .setFailureCount(getFailures().size())
        .build();
  }
}
```

### Long-Running Operation Support

```java
// AsyncOperationManager.java
public class AsyncOperationManager {

  private final ConcurrentHashMap<String, OperationStatus> operations;

  public String submitOperation(Callable<OperationResult> operation) {
    String operationId = UUID.randomUUID().toString();

    OperationStatus status = OperationStatus.builder()
        .setId(operationId)
        .setState(OperationState.RUNNING)
        .setStartTime(Instant.now())
        .build();

    operations.put(operationId, status);

    CompletableFuture.supplyAsync(() -> {
      try {
        OperationResult result = operation.call();
        status.complete(result);
        return result;
      } catch (Exception e) {
        status.fail(e);
        throw new CompletionException(e);
      }
    });

    return operationId;
  }

  public OperationStatus getStatus(String operationId) {
    return operations.get(operationId);
  }
}

// Usage in endpoint
@POST
@Path("/:async")
public Response createTopicsAsync(
    @PathParam("clusterId") String clusterId,
    BulkTopicCreateRequest request) {

  String operationId = asyncOperationManager.submitOperation(() ->
      createTopicsBulk(clusterId, request));

  return Response.accepted()
      .entity(Map.of("operation_id", operationId))
      .header("Location", "/v3/operations/" + operationId)
      .build();
}

@GET
@Path("/operations/{operationId}")
public OperationStatus getOperationStatus(
    @PathParam("operationId") String operationId) {

  OperationStatus status = asyncOperationManager.getStatus(operationId);
  if (status == null) {
    throw new NotFoundException("Operation not found");
  }
  return status;
}
```

### Bulk Delete

```java
// BulkTopicDeleteAction.java
@DELETE
@Path("/:batch")
public void deleteTopicsBatch(
    @Suspended AsyncResponse asyncResponse,
    @PathParam("clusterId") String clusterId,
    @QueryParam("topics") List<String> topicNames) {

  if (topicNames.size() > 1000) {
    throw new BadRequestException("Maximum 1000 topics per batch");
  }

  Map<String, CompletableFuture<Void>> deleteFutures = topicNames.stream()
      .collect(toMap(
          Function.identity(),
          topic -> topicManager.deleteTopic(clusterId, topic)));

  CompletableFuture.allOf(deleteFutures.values().toArray(new CompletableFuture[0]))
      .thenApply(v -> {
        List<String> successes = new ArrayList<>();
        List<OperationFailure> failures = new ArrayList<>();

        deleteFutures.forEach((topic, future) -> {
          try {
            future.join();
            successes.add(topic);
          } catch (Exception e) {
            failures.add(OperationFailure.of(topic, e.getMessage()));
          }
        });

        return BulkDeleteResponse.builder()
            .setSuccesses(successes)
            .setFailures(failures)
            .build();
      })
      .whenComplete((response, error) -> {
        if (error != null) {
          asyncResponse.resume(error);
        } else {
          asyncResponse.resume(Response.ok(response).build());
        }
      });
}
```

### Configuration

```properties
# Bulk operation limits
admin.bulk.max.batch.size=1000
admin.bulk.parallel.operations=50

# Async operation tracking
admin.async.operation.ttl.ms=3600000  # 1 hour
admin.async.operation.cleanup.interval.ms=300000  # 5 minutes

# Operation timeouts
admin.operation.timeout.ms=60000
```

## Example Usage

### Before (Sequential)

```bash
# Create 100 topics - 100 requests
for i in {1..100}; do
  curl -X POST http://localhost:8082/v3/clusters/abc/topics \
    -d "{\"topic_name\": \"topic-$i\", \"partitions_count\": 3}"
done
# Time: ~10 minutes
```

### After (Bulk Sync)

```bash
# Create 100 topics - 1 request
curl -X POST http://localhost:8082/v3/clusters/abc/topics/:batch \
  -d '{
    "topics": [
      {"topic_name": "topic-1", "partitions_count": 3},
      {"topic_name": "topic-2", "partitions_count": 3},
      ...
      {"topic_name": "topic-100", "partitions_count": 3}
    ]
  }'

# Response:
{
  "successes": [...98 topics...],
  "failures": [
    {"topic": "topic-42", "error": "Topic already exists"},
    {"topic": "topic-87", "error": "Invalid partition count"}
  ],
  "summary": {
    "total": 100,
    "success_count": 98,
    "failure_count": 2
  }
}

# Time: ~30 seconds
```

### After (Bulk Async)

```bash
# Submit async operation
curl -X POST http://localhost:8082/v3/clusters/abc/topics/:async \
  -d '{"topics": [...]}'

# Response: 202 Accepted
{
  "operation_id": "op_abc123",
  "status": "running"
}

# Check status
curl http://localhost:8082/v3/operations/op_abc123

# Response:
{
  "operation_id": "op_abc123",
  "state": "completed",
  "start_time": "2025-11-19T10:00:00Z",
  "end_time": "2025-11-19T10:00:30Z",
  "progress": {
    "total": 100,
    "completed": 100,
    "percentage": 100
  },
  "result": {
    "success_count": 98,
    "failure_count": 2
  }
}
```

## Implementation Plan

### Phase 1: Bulk APIs (Week 1-2)

1. Bulk topic create
2. Bulk topic delete
3. Bulk config update

### Phase 2: Async Operations (Week 2-3)

1. `AsyncOperationManager`
2. Operation status tracking
3. Operation cleanup

### Phase 3: Additional Bulk Operations (Week 3)

1. Bulk ACL operations
2. Bulk partition operations
3. Bulk consumer group operations

## Backwards Compatibility

- **New endpoints:** `/:batch` and `/:async` suffixes
- **Existing APIs:** Unchanged
- **Migration:** Optional, use when beneficial

## Alternatives Considered

### Alternative 1: GraphQL

Use GraphQL for bulk operations.

**Rejected because:**
- Major API paradigm shift
- Existing REST clients incompatible
- Overkill for this use case

### Alternative 2: Kafka Transactions

Use Kafka transactions for atomicity.

**Rejected because:**
- Admin operations don't support transactions
- Overkill for most operations
- Best-effort is acceptable

## Open Questions

1. **Atomicity:** Should bulk operations be all-or-nothing?
   - Proposal: Best-effort with partial success/failure

2. **Ordering:** Should operations be ordered?
   - Proposal: No guaranteed order (parallel execution)

## Success Criteria

- [ ] 10x faster bulk operations
- [ ] Partial success handling
- [ ] Progress tracking for long operations
- [ ] < 1 minute for 1000 topic creation

## Effort Estimation

**Total:** 15 dev-days

| Task | Days |
|------|------|
| Bulk API design | 2 |
| Bulk topic operations | 4 |
| Async operation framework | 4 |
| Other bulk operations | 3 |
| Testing | 2 |

## Required Approvals

- [ ] Architecture Review
- [ ] API Design Review
