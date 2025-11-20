# RFC-0008: Graceful Shutdown with Request Draining

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Implement graceful shutdown with request draining to prevent data loss and improve reliability during deployments and scaling operations.

## Motivation

Currently, REST Proxy has no graceful shutdown mechanism:

```java
// KafkaRestMain.java
public static void main(String[] args) {
  KafkaRestApplication app = new KafkaRestApplication(config);
  app.start();
  app.join();  // Blocks until shutdown
}
```

**Problems during shutdown (SIGTERM):**

1. **In-flight requests dropped** - Produce requests in progress are lost
2. **Producer buffer loss** - Buffered messages not flushed
3. **Consumer position loss** - Offsets not committed
4. **Zero-downtime deployment fails** - Kubernetes rolling updates cause errors

**Real-world impact:**
- Kubernetes deployment: SIGTERM sent, 30s grace period
- 100 in-flight produce requests → 100 lost messages
- Customer impact: Data loss, failed transactions
- Frequency: Every deployment (multiple times per day)

## Detailed Design

### Shutdown Lifecycle

```
1. Receive SIGTERM
2. Mark as "draining" (reject new requests with 503)
3. Wait for in-flight requests to complete (up to timeout)
4. Flush producer buffers
5. Commit consumer offsets
6. Close connections
7. Exit
```

### Implementation

```java
public class GracefulShutdownHandler {

  private final AtomicBoolean draining = new AtomicBoolean(false);
  private final CountDownLatch inflightRequests = new CountDownLatch(0);
  private final KafkaRestApplication app;
  private final KafkaRestConfig config;

  public void initiateShutdown() {
    log.info("Initiating graceful shutdown");
    draining.set(true);

    // Stop accepting new requests
    stopAcceptingRequests();

    // Wait for in-flight requests
    boolean completed = waitForInflightRequests();
    if (!completed) {
      log.warn("Timeout waiting for requests, forcing shutdown");
    }

    // Flush producer
    flushProducer();

    // Commit consumer offsets
    commitConsumerOffsets();

    // Shutdown application
    app.stop();
  }

  private void stopAcceptingRequests() {
    // Register filter to reject new requests
    app.getServer().setHandler(new DrainingHandler(
        app.getServer().getHandler()));
  }

  private boolean waitForInflightRequests() {
    long timeoutMs = config.getLong("shutdown.drain.timeout.ms");
    try {
      return inflightRequests.await(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void flushProducer() {
    try {
      Producer<byte[], byte[]> producer =
          app.getContext().getProducer();
      producer.flush();
      log.info("Producer flushed successfully");
    } catch (Exception e) {
      log.error("Error flushing producer", e);
    }
  }

  private void commitConsumerOffsets() {
    // Commit all consumer offsets
    app.getConsumerManager().commitAll();
  }
}
```

### Request Tracking Filter

```java
@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION - 100)
public class InflightRequestTracker implements ContainerRequestFilter,
                                                ContainerResponseFilter {

  @Inject
  private GracefulShutdownHandler shutdownHandler;

  @Override
  public void filter(ContainerRequestContext requestContext) {
    if (shutdownHandler.isDraining()) {
      // Reject new requests
      requestContext.abortWith(
          Response.status(503)
              .entity(Map.of(
                  "error", "Service is shutting down",
                  "retry_after", 5))
              .build());
      return;
    }

    // Track in-flight request
    shutdownHandler.incrementInflight();
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
                    ContainerResponseContext responseContext) {
    // Request completed
    shutdownHandler.decrementInflight();
  }
}
```

### Kubernetes Integration

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: kafka-rest
          lifecycle:
            preStop:
              exec:
                # Send graceful shutdown signal
                command: ["/bin/sh", "-c", "kill -TERM 1; sleep 30"]
          # Termination grace period
          terminationGracePeriodSeconds: 60
```

### Configuration

```properties
# Enable graceful shutdown
shutdown.graceful.enabled=true

# Max time to wait for in-flight requests
shutdown.drain.timeout.ms=30000

# Return 503 during draining
shutdown.reject.new.requests=true

# Producer flush timeout
shutdown.producer.flush.timeout.ms=10000

# Consumer commit timeout
shutdown.consumer.commit.timeout.ms=5000
```

### Metrics

```java
// Shutdown metrics
shutdownInitiated.increment();
drainingDuration.record(duration);
inflightRequestsAtShutdown.record(count);
requestsDuringDraining.increment();
successfulShutdowns.increment();
forcedShutdowns.increment();
```

## Example Usage

### Before

```bash
# During Kubernetes rolling update
kubectl rollout restart deployment/kafka-rest

# Result:
# - 100 in-flight requests: LOST
# - Producer buffer (1000 messages): LOST
# - Consumer offsets: NOT COMMITTED
# - Error rate spike: 503s, timeouts
```

### After

```bash
# During Kubernetes rolling update
kubectl rollout restart deployment/kafka-rest

# Result:
# 1. New requests → 503 "Service is shutting down"
# 2. In-flight requests → Complete normally
# 3. Producer buffer → Flushed to Kafka
# 4. Consumer offsets → Committed
# 5. Clean shutdown after 5-10 seconds
# 6. Zero data loss
```

### Observability

```
# Log output during shutdown
INFO  [shutdown-thread] Initiating graceful shutdown
INFO  [shutdown-thread] Draining mode enabled, rejecting new requests
INFO  [shutdown-thread] Waiting for 47 in-flight requests
INFO  [shutdown-thread] All requests completed in 8.3 seconds
INFO  [shutdown-thread] Flushing producer buffer (234 messages)
INFO  [shutdown-thread] Producer flushed successfully
INFO  [shutdown-thread] Committing consumer offsets
INFO  [shutdown-thread] Shutdown completed gracefully
```

## Implementation Plan

### Phase 1: Core Shutdown Logic (Week 1)

1. Create `GracefulShutdownHandler`
2. Add SIGTERM signal handler
3. Implement request tracking

### Phase 2: Resource Cleanup (Week 2)

1. Producer flush logic
2. Consumer offset commit
3. Connection cleanup

### Phase 3: Testing and Validation (Week 3)

1. Integration tests with Kafka
2. Kubernetes deployment tests
3. Load testing during shutdown

## Backwards Compatibility

- **Default:** Graceful shutdown enabled
- **Opt-out:** `shutdown.graceful.enabled=false`
- **Fallback:** Hard shutdown after timeout

## Alternatives Considered

### Alternative 1: Load Balancer Draining

Rely on load balancer to drain connections.

**Rejected because:**
- Doesn't handle producer buffer flush
- Doesn't commit consumer offsets
- Load balancer unaware of in-flight requests

### Alternative 2: Longer Termination Grace Period

Increase Kubernetes `terminationGracePeriodSeconds`.

**Rejected because:**
- Doesn't actively drain requests
- Wastes time waiting
- Still loses buffered data

## Open Questions

1. **Timeout behavior:** What if requests take longer than timeout?
   - Proposal: Forcibly close after timeout, log warnings

2. **New request handling:** Should we queue or reject?
   - Proposal: Reject with 503 + Retry-After header

3. **Health check during draining:** Should readiness return unhealthy?
   - Proposal: Yes, return 503 to signal draining

## Success Criteria

- [ ] Zero data loss during deployments
- [ ] 100% of in-flight requests complete successfully
- [ ] Producer buffers fully flushed
- [ ] Consumer offsets committed
- [ ] Kubernetes rolling updates succeed without errors
- [ ] Metrics show clean shutdowns

## Effort Estimation

**Total:** 15 dev-days

| Task | Days |
|------|------|
| Design and review | 2 |
| Shutdown handler implementation | 4 |
| Request tracking | 2 |
| Producer/consumer cleanup | 3 |
| Kubernetes integration | 2 |
| Testing | 2 |

## Required Approvals

- [ ] Architecture Review
- [ ] Operations Team (Deployment impact)
