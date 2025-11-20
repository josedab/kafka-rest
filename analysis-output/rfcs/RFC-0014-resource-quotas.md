# RFC-0014: Per-Consumer/Client Resource Quotas

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Implement per-consumer and per-client resource quotas to ensure fair resource sharing and prevent individual clients from monopolizing REST Proxy resources.

## Motivation

Currently, all clients share resources without quotas:

**Problems:**

1. **Noisy neighbor** - One client can consume all threads
2. **Unfair resource allocation** - No guaranteed capacity per tenant
3. **DoS by single client** - Create 1000 consumers, exhaust resources
4. **No isolation** - One slow client affects all others

**Real-world scenario:**
- Client A creates 100 consumers
- Client B cannot create any (thread pool exhausted)
- Client A monopolizes all resources
- **Solution needed:** Fair quotas

## Detailed Design

### Quota Framework

```java
public class ResourceQuotaManager {

  private final ConcurrentHashMap<String, ClientQuota> clientQuotas;
  private final KafkaRestConfig config;

  public static class ClientQuota {
    private final AtomicInteger consumerCount = new AtomicInteger(0);
    private final AtomicInteger produceRequestsPerSecond = new AtomicInteger(0);
    private final AtomicLong bytesProducedPerSecond = new AtomicLong(0);
    private final int maxConsumers;
    private final int maxProduceRps;
    private final long maxProduceBytesPerSecond;

    public boolean canCreateConsumer() {
      return consumerCount.get() < maxConsumers;
    }

    public void createConsumer() {
      if (!canCreateConsumer()) {
        throw new QuotaExceededException(
            "Consumer quota exceeded: " + consumerCount.get() + "/" + maxConsumers);
      }
      consumerCount.incrementAndGet();
    }

    public void deleteConsumer() {
      consumerCount.decrementAndGet();
    }

    public boolean canProduce(int bytes) {
      // Check both request count and byte quota
      return produceRequestsPerSecond.get() < maxProduceRps &&
             bytesProducedPerSecond.get() + bytes < maxProduceBytesPerSecond;
    }

    public void recordProduce(int bytes) {
      if (!canProduce(bytes)) {
        throw new QuotaExceededException("Produce quota exceeded");
      }
      produceRequestsPerSecond.incrementAndGet();
      bytesProducedPerSecond.addAndGet(bytes);
    }

    // Reset counters every second
    public void reset() {
      produceRequestsPerSecond.set(0);
      bytesProducedPerSecond.set(0);
    }
  }

  public ClientQuota getQuota(String clientId) {
    return clientQuotas.computeIfAbsent(clientId, id -> {
      // Check for client-specific quota configuration
      ClientQuota quota = loadClientQuota(id);
      if (quota == null) {
        // Use default quota
        quota = createDefaultQuota();
      }
      return quota;
    });
  }

  private ClientQuota loadClientQuota(String clientId) {
    // Load from configuration
    // Example: quota.client.premium.max.consumers=1000
    String prefix = "quota.client." + clientId + ".";
    if (config.containsKey(prefix + "max.consumers")) {
      return new ClientQuota(
          config.getInt(prefix + "max.consumers"),
          config.getInt(prefix + "max.produce.rps"),
          config.getLong(prefix + "max.produce.bytes.per.second"));
    }
    return null;
  }

  private ClientQuota createDefaultQuota() {
    return new ClientQuota(
        config.getInt("quota.default.max.consumers"),
        config.getInt("quota.default.max.produce.rps"),
        config.getLong("quota.default.max.produce.bytes.per.second"));
  }
}
```

### Client Identification

```java
@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION - 50)
public class ClientIdentificationFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) {
    // Extract client ID from various sources
    String clientId = extractClientId(requestContext);

    // Store in request context
    requestContext.setProperty("client_id", clientId);

    // Add to MDC for logging
    MDC.put("client_id", clientId);
  }

  private String extractClientId(ContainerRequestContext context) {
    // 1. Custom header (preferred)
    String clientId = context.getHeaderString("X-Client-ID");
    if (clientId != null) {
      return clientId;
    }

    // 2. API key
    String apiKey = context.getHeaderString("X-API-Key");
    if (apiKey != null) {
      return lookupClientByApiKey(apiKey);
    }

    // 3. OAuth token
    String authorization = context.getHeaderString("Authorization");
    if (authorization != null && authorization.startsWith("Bearer ")) {
      return extractClientFromToken(authorization);
    }

    // 4. IP address (fallback)
    return context.getHeaderString("X-Forwarded-For");
  }
}
```

### Quota Enforcement

```java
// Consumer creation with quota check
public class ConsumerManager {

  @Inject
  private ResourceQuotaManager quotaManager;

  public ConsumerInstanceId createConsumer(
      String groupId,
      String instanceId,
      String clientId) {

    // Check quota
    ClientQuota quota = quotaManager.getQuota(clientId);
    quota.createConsumer();

    try {
      // Create consumer
      KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer();
      ConsumerInstanceId id = new ConsumerInstanceId(groupId, instanceId);
      consumers.put(id, new ConsumerState(consumer, quota));
      return id;
    } catch (Exception e) {
      // Roll back quota
      quota.deleteConsumer();
      throw e;
    }
  }

  public void deleteConsumer(ConsumerInstanceId id) {
    ConsumerState state = consumers.remove(id);
    if (state != null) {
      state.getConsumer().close();
      state.getQuota().deleteConsumer();
    }
  }
}

// Produce with quota check
public class ProduceController {

  @Inject
  private ResourceQuotaManager quotaManager;

  @Override
  public CompletableFuture<ProduceResult> produce(
      String clusterId,
      String topicName,
      String clientId,
      ProduceRequest request) {

    // Estimate message size
    int messageSize = estimateSize(request);

    // Check quota
    ClientQuota quota = quotaManager.getQuota(clientId);
    quota.recordProduce(messageSize);

    // Proceed with produce
    return super.produce(clusterId, topicName, request);
  }
}
```

### Configuration

```properties
# Default quotas (applied to all clients)
quota.default.max.consumers=10
quota.default.max.produce.rps=100
quota.default.max.produce.bytes.per.second=10485760  # 10 MB/s

# Client-specific quotas
quota.client.premium-client.max.consumers=1000
quota.client.premium-client.max.produce.rps=10000
quota.client.premium-client.max.produce.bytes.per.second=104857600  # 100 MB/s

quota.client.basic-client.max.consumers=5
quota.client.basic-client.max.produce.rps=10
quota.client.basic-client.max.produce.bytes.per.second=1048576  # 1 MB/s

# Quota reset interval
quota.reset.interval.ms=1000

# Client identification
quota.client.id.source=header  # header, api-key, oauth, ip
quota.client.id.header=X-Client-ID
```

### Quota Metrics

```java
// Per-client metrics
clientConsumerCount.labels(clientId).set(quota.getConsumerCount());
clientProduceRps.labels(clientId).set(quota.getProduceRps());
clientProduceBytesPerSecond.labels(clientId).set(quota.getProduceBytesPerSecond());
clientQuotaViolations.labels(clientId).increment();

// Aggregated metrics
totalQuotaViolations.increment();
quotaUtilization.record(quota.getUtilization());
```

### Quota Violation Response

```json
{
  "error_code": 42901,
  "message": "Client quota exceeded",
  "client_id": "basic-client",
  "quota_type": "consumer_count",
  "current": 5,
  "limit": 5,
  "retry_after": 60
}
```

## Example Usage

### Before (No Quotas)

```bash
# Client A creates 100 consumers
for i in {1..100}; do
  curl -X POST http://localhost:8082/consumers/group1/instances \
    -H "X-Client-ID: client-a"
done
# Success: All 100 consumers created

# Client B tries to create consumers
curl -X POST http://localhost:8082/consumers/group2/instances \
  -H "X-Client-ID: client-b"
# Error: Thread pool exhausted (Client A used all resources)
```

### After (With Quotas)

```bash
# Client A creates consumers (limit: 10)
for i in {1..100}; do
  curl -X POST http://localhost:8082/consumers/group1/instances \
    -H "X-Client-ID: client-a"
done
# First 10 succeed, remaining 90 fail with quota exceeded

# Client B can create consumers (has own quota)
curl -X POST http://localhost:8082/consumers/group2/instances \
  -H "X-Client-ID: client-b"
# Success: Client B has guaranteed capacity
```

## Implementation Plan

### Phase 1: Quota Framework (Week 1-2)

1. Create `ResourceQuotaManager`
2. Implement `ClientQuota`
3. Configuration loading

### Phase 2: Client Identification (Week 2)

1. `ClientIdentificationFilter`
2. Multiple ID sources (header, API key, OAuth)
3. Fallback to IP

### Phase 3: Enforcement (Week 3)

1. Consumer quota enforcement
2. Produce quota enforcement
3. Metrics and monitoring

### Phase 4: Testing (Week 4)

1. Load testing with quotas
2. Multi-tenant scenarios
3. Quota violation handling

## Backwards Compatibility

- **Default:** High quotas (backward compatible)
- **Opt-in:** Lower quotas via configuration
- **Gradual:** Roll out to specific clients first

## Alternatives Considered

### Alternative 1: Kafka-Level Quotas

Use Kafka broker quotas.

**Rejected because:**
- Doesn't protect REST Proxy resources
- Client identity may not propagate
- Coarse-grained

### Alternative 2: API Gateway Quotas

Implement at API gateway.

**Rejected because:**
- Gateway unaware of resource types (consumers vs produces)
- Can't enforce per-consumer quotas
- Still need internal protection

## Open Questions

1. **Dynamic quota updates:** Should quotas be updatable without restart?
   - Proposal: Yes, via admin API

2. **Quota inheritance:** Should group/tenant quotas exist?
   - Proposal: Future enhancement

## Success Criteria

- [ ] Fair resource allocation across clients
- [ ] No single client can monopolize resources
- [ ] Quota violations logged and monitored
- [ ] < 1ms quota check overhead

## Effort Estimation

**Total:** 20 dev-days

| Task | Days |
|------|------|
| Quota framework | 5 |
| Client identification | 3 |
| Enforcement | 5 |
| Metrics | 2 |
| Testing | 4 |
| Documentation | 1 |

## Required Approvals

- [ ] Architecture Review
- [ ] Product Team (Quota values)
