# RFC-0004: Health Check Endpoint

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Add dedicated health check endpoints for liveness and readiness probes, enabling proper Kubernetes deployment and load balancer health checks.

## Motivation

REST Proxy lacks dedicated health endpoints, making it difficult to:

1. **Kubernetes deployments** - No readiness/liveness probes
2. **Load balancer health** - Must use application endpoints
3. **Monitoring** - No dedicated endpoint for health status
4. **Dependency checks** - Can't verify Kafka/SR connectivity

**Current workaround:**
```bash
curl http://localhost:8082/v3/clusters
# Returns 200 if working, but also returns data
# Load balancer sees "unhealthy" on first request (cache miss latency)
```

## Detailed Design

### Endpoint Structure

```
GET /health           # Overall health
GET /health/live      # Liveness (process running)
GET /health/ready     # Readiness (can serve traffic)
```

### Implementation

```java
@Path("/health")
@ResourceName("api.health")
@DoNotRateLimit
public class HealthResource {

  @Inject
  private Provider<Admin> adminProvider;

  @Inject
  private Provider<Optional<SchemaRegistryClient>> schemaRegistryProvider;

  @Inject
  private KafkaRestConfig config;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public HealthResponse health() {
    HealthResponse.Builder builder = HealthResponse.builder()
        .setStatus(HealthStatus.UP);

    // Check Kafka
    HealthCheck kafkaCheck = checkKafka();
    builder.addCheck("kafka", kafkaCheck);

    // Check Schema Registry (if configured)
    if (config.getString("schema.registry.url") != null) {
      HealthCheck srCheck = checkSchemaRegistry();
      builder.addCheck("schema_registry", srCheck);
    }

    HealthResponse response = builder.build();

    // Overall status is UP only if all checks pass
    if (response.getChecks().values().stream()
        .anyMatch(c -> c.getStatus() != HealthStatus.UP)) {
      return response.withStatus(HealthStatus.DOWN);
    }

    return response;
  }

  @GET
  @Path("/live")
  @Produces(MediaType.APPLICATION_JSON)
  public Response liveness() {
    // Always return 200 if process is running
    return Response.ok(Map.of("status", "UP")).build();
  }

  @GET
  @Path("/ready")
  @Produces(MediaType.APPLICATION_JSON)
  public Response readiness() {
    try {
      // Check Kafka connectivity
      Admin admin = adminProvider.get();
      admin.describeCluster()
          .clusterId()
          .get(config.getInt("health.readiness.timeout.ms"), TimeUnit.MILLISECONDS);

      return Response.ok(Map.of("status", "UP")).build();
    } catch (Exception e) {
      return Response.status(503)
          .entity(Map.of("status", "DOWN", "reason", e.getMessage()))
          .build();
    }
  }

  private HealthCheck checkKafka() {
    try {
      Admin admin = adminProvider.get();
      String clusterId = admin.describeCluster()
          .clusterId()
          .get(5, TimeUnit.SECONDS);

      return HealthCheck.builder()
          .setStatus(HealthStatus.UP)
          .setDetails(Map.of("cluster_id", clusterId))
          .build();
    } catch (Exception e) {
      return HealthCheck.builder()
          .setStatus(HealthStatus.DOWN)
          .setDetails(Map.of("error", e.getMessage()))
          .build();
    }
  }

  private HealthCheck checkSchemaRegistry() {
    try {
      SchemaRegistryClient client = schemaRegistryProvider.get().orElseThrow();
      Collection<String> subjects = client.getAllSubjects();

      return HealthCheck.builder()
          .setStatus(HealthStatus.UP)
          .setDetails(Map.of("subjects_count", subjects.size()))
          .build();
    } catch (Exception e) {
      return HealthCheck.builder()
          .setStatus(HealthStatus.DOWN)
          .setDetails(Map.of("error", e.getMessage()))
          .build();
    }
  }
}
```

### Response Format

```json
{
  "status": "UP",
  "checks": {
    "kafka": {
      "status": "UP",
      "details": {
        "cluster_id": "abc123"
      }
    },
    "schema_registry": {
      "status": "UP",
      "details": {
        "subjects_count": 42
      }
    }
  }
}
```

### Configuration

```properties
# Health check timeouts
health.readiness.timeout.ms=5000
health.kafka.timeout.ms=5000
health.schema.registry.timeout.ms=5000

# Skip checks (for testing)
health.kafka.enabled=true
health.schema.registry.enabled=true
```

## Example Usage

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: kafka-rest
          livenessProbe:
            httpGet:
              path: /health/live
              port: 8082
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health/ready
              port: 8082
            initialDelaySeconds: 15
            periodSeconds: 5
```

### Load Balancer Health Check

```
Health Check Path: /health/ready
Healthy threshold: 2
Unhealthy threshold: 3
Timeout: 5 seconds
Interval: 10 seconds
```

### Monitoring

```bash
# Check overall health
curl http://localhost:8082/health | jq .status

# Alert on non-UP status
if [ "$(curl -s http://localhost:8082/health | jq -r .status)" != "UP" ]; then
  alert "REST Proxy unhealthy"
fi
```

## Implementation Plan

### Phase 1: Basic Endpoints (Day 1-2)

1. Create `HealthResource` class
2. Implement `/health/live` (trivial)
3. Register in `V3ResourcesFeature`

### Phase 2: Dependency Checks (Day 2-3)

1. Implement Kafka connectivity check
2. Implement Schema Registry check
3. Add configuration properties

### Phase 3: Testing (Day 3)

1. Unit tests
2. Integration tests
3. Documentation

## Backwards Compatibility

- **New endpoints** - No existing functionality affected
- **Default:** Enabled
- **Access control:** Included in allowlist patterns

## Alternatives Considered

### Alternative 1: Use Existing Endpoints

Use `/v3/clusters` as health check.

**Rejected because:**
- Returns unnecessary data
- Higher latency
- Can't distinguish liveness vs readiness

### Alternative 2: JMX Health Metrics

Expose health via JMX.

**Rejected because:**
- Not HTTP-based
- Harder for Kubernetes
- Requires JMX agent

## Open Questions

1. **Authentication:** Should health endpoints require auth?
   - Proposal: No, for load balancer compatibility

2. **Caching:** Should we cache health results?
   - Proposal: No caching, always fresh checks

3. **Detailed errors:** How much detail in error messages?
   - Proposal: Minimal in response, detailed in logs

## Success Criteria

- [ ] Kubernetes probes working
- [ ] Load balancer health checks passing
- [ ] < 100ms latency for liveness
- [ ] < 5s latency for readiness
- [ ] Clear error messages on failure

## Effort Estimation

**Total:** 3 dev-days

| Task | Days |
|------|------|
| Implementation | 1.5 |
| Testing | 1 |
| Documentation | 0.5 |

## Required Approvals

- [ ] Operations Team
