# RFC-0011: Structured Logging with MDC

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Implement structured logging with Mapped Diagnostic Context (MDC) to enable correlation, filtering, and analysis of logs across distributed systems.

## Motivation

Current logging is unstructured:

```java
log.info("Processing request for topic {}", topicName);
log.error("Failed to produce message", exception);
```

**Problems:**
1. **No correlation** - Can't link related log entries across services
2. **No filtering** - Can't search logs by cluster_id, user, or request_id
3. **No context** - Logs missing important metadata
4. **Hard to parse** - Inconsistent formats

**Example problem:**
```
[ERROR] Failed to produce message
  at ProduceController.produce(ProduceController.java:123)

Questions:
- Which cluster?
- Which topic?
- Which user?
- Related to which HTTP request?
→ Impossible to answer without manual correlation
```

## Detailed Design

### MDC Filter

```java
@Provider
@PreMatching
@Priority(Priorities.HEADER_DECORATOR - 50)
public class MDCFilter implements ContainerRequestFilter,
                                   ContainerResponseFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) {
    // Generate or extract request ID
    String requestId = requestContext.getHeaderString("X-Request-ID");
    if (requestId == null) {
      requestId = UUID.randomUUID().toString();
    }

    // Populate MDC
    MDC.put("request_id", requestId);
    MDC.put("http_method", requestContext.getMethod());
    MDC.put("http_path", requestContext.getUriInfo().getPath());
    MDC.put("http_client_ip", getClientIp(requestContext));
    MDC.put("user_agent", requestContext.getHeaderString("User-Agent"));

    // Extract custom headers
    String userId = requestContext.getHeaderString("X-User-ID");
    if (userId != null) {
      MDC.put("user_id", userId);
    }

    String tenantId = requestContext.getHeaderString("X-Tenant-ID");
    if (tenantId != null) {
      MDC.put("tenant_id", tenantId);
    }

    // Add request ID to response headers for client correlation
    requestContext.setProperty("request_id", requestId);
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
                    ContainerResponseContext responseContext) {
    // Add request ID to response
    String requestId = (String) requestContext.getProperty("request_id");
    responseContext.getHeaders().putSingle("X-Request-ID", requestId);

    // Add response status to MDC
    MDC.put("http_status", String.valueOf(responseContext.getStatus()));

    // Clear MDC after request
    MDC.clear();
  }
}
```

### Domain Context Enhancement

```java
public class ProduceControllerImpl implements ProduceController {

  @Override
  public CompletableFuture<ProduceResult> produce(
      String clusterId,
      String topicName,
      /* ... */) {

    // Add domain context to MDC
    MDC.put("cluster_id", clusterId);
    MDC.put("topic_name", topicName);
    MDC.put("operation", "produce");

    try {
      log.info("Starting produce operation");
      // Now includes: request_id, cluster_id, topic_name, operation

      Producer<byte[], byte[]> producer = producerProvider.get();
      // ... rest of implementation

      log.info("Produce operation completed successfully");
    } catch (Exception e) {
      log.error("Produce operation failed", e);
      throw e;
    } finally {
      // Clean up domain context
      MDC.remove("cluster_id");
      MDC.remove("topic_name");
      MDC.remove("operation");
    }
  }
}
```

### JSON Log Format

```xml
<!-- log4j2.xml -->
<Configuration>
  <Appenders>
    <Console name="Console">
      <JsonTemplateLayout eventTemplateUri="classpath:JsonLayout.json"/>
    </Console>
  </Appenders>

  <Loggers>
    <Root level="info">
      <AppenderRef ref="Console"/>
    </Root>
  </Loggers>
</Configuration>
```

```json
// JsonLayout.json
{
  "timestamp": "${date:yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}",
  "level": "${level}",
  "logger": "${logger}",
  "thread": "${thread}",
  "message": "${message}",
  "exception": "${exception:format=full}",
  "mdc": {
    "request_id": "${mdc:request_id}",
    "cluster_id": "${mdc:cluster_id}",
    "topic_name": "${mdc:topic_name}",
    "operation": "${mdc:operation}",
    "user_id": "${mdc:user_id}",
    "tenant_id": "${mdc:tenant_id}",
    "http_method": "${mdc:http_method}",
    "http_path": "${mdc:http_path}",
    "http_status": "${mdc:http_status}",
    "http_client_ip": "${mdc:http_client_ip}"
  }
}
```

### Async Context Propagation

```java
// For CompletableFuture
public class MDCPropagatingCompletableFuture {

  public static <T> CompletableFuture<T> supplyAsync(
      Supplier<T> supplier,
      Executor executor) {

    // Capture current MDC
    Map<String, String> contextMap = MDC.getCopyOfContextMap();

    return CompletableFuture.supplyAsync(() -> {
      // Restore MDC in async thread
      if (contextMap != null) {
        MDC.setContextMap(contextMap);
      }
      try {
        return supplier.get();
      } finally {
        MDC.clear();
      }
    }, executor);
  }
}
```

### Configuration

```properties
# Structured logging
logging.format=json
logging.mdc.enabled=true

# Include/exclude MDC fields
logging.mdc.include=request_id,cluster_id,topic_name,user_id
logging.mdc.exclude=user_agent

# Request ID header name
logging.request.id.header=X-Request-ID

# Log sampling (for high-volume endpoints)
logging.sample.rate=1.0
```

## Example Usage

### Before

```
2025-11-19 10:30:00 [INFO] Processing request for topic orders
2025-11-19 10:30:00 [INFO] Schema resolved for subject orders-value
2025-11-19 10:30:00 [ERROR] Failed to produce message
  at ProduceController.produce(ProduceController.java:123)
```

**Analysis:** Which request? Which cluster? Which user?

### After (JSON)

```json
{
  "timestamp": "2025-11-19T10:30:00.000Z",
  "level": "INFO",
  "logger": "io.confluent.kafkarest.controllers.ProduceControllerImpl",
  "message": "Starting produce operation",
  "mdc": {
    "request_id": "req_abc123xyz",
    "cluster_id": "cluster_prod",
    "topic_name": "orders",
    "operation": "produce",
    "user_id": "user_123",
    "tenant_id": "tenant_acme",
    "http_method": "POST",
    "http_path": "/v3/clusters/cluster_prod/topics/orders/records",
    "http_client_ip": "192.168.1.100"
  }
}

{
  "timestamp": "2025-11-19T10:30:00.050Z",
  "level": "ERROR",
  "logger": "io.confluent.kafkarest.controllers.ProduceControllerImpl",
  "message": "Produce operation failed",
  "exception": "TimeoutException: Request timed out...",
  "mdc": {
    "request_id": "req_abc123xyz",
    "cluster_id": "cluster_prod",
    "topic_name": "orders",
    "operation": "produce"
  }
}
```

**Analysis:**
```bash
# Find all logs for this request across all services
grep "req_abc123xyz" *.log

# Find all errors for this cluster
jq 'select(.mdc.cluster_id == "cluster_prod" and .level == "ERROR")' logs.json

# Find all requests from this user
jq 'select(.mdc.user_id == "user_123")' logs.json
```

### Query Examples (Elasticsearch/Loki)

```
# All errors for a specific request
mdc.request_id:"req_abc123xyz" AND level:"ERROR"

# All produce operations that failed
mdc.operation:"produce" AND level:"ERROR"

# All requests from a tenant
mdc.tenant_id:"tenant_acme"

# Slow requests (duration > 1s)
mdc.http_status:200 AND duration_ms:[1000 TO *]
```

## Implementation Plan

### Phase 1: MDC Infrastructure (Week 1)

1. Create `MDCFilter`
2. Configure JSON logging
3. Update log4j2.xml

### Phase 2: Domain Context (Week 2)

1. Add MDC to controllers
2. Async context propagation
3. Schema Registry context

### Phase 3: Rollout (Week 3)

1. Testing and validation
2. Log aggregation setup (ELK/Loki)
3. Dashboard creation

## Backwards Compatibility

- **Default:** JSON logging enabled
- **Fallback:** Plain text format available
- **Migration:** Gradual rollout

## Alternatives Considered

### Alternative 1: External Logging Agent

Use Fluentd/Filebeat to add structure.

**Rejected because:**
- Less accurate (regex parsing)
- Missing internal context
- Higher latency

### Alternative 2: Correlation ID Only

Just add correlation ID to logs.

**Rejected because:**
- Still unstructured
- Missing domain context
- Hard to query

## Open Questions

1. **PII in logs:** Should we redact user data?
   - Proposal: Configurable redaction for sensitive fields

2. **Log volume:** Will JSON increase storage?
   - Proposal: Yes, but compression helps (gzip)

## Success Criteria

- [ ] All requests have correlation IDs
- [ ] 100% of logs in JSON format
- [ ] Sub-second log queries
- [ ] Dashboards showing request traces

## Effort Estimation

**Total:** 15 dev-days

| Task | Days |
|------|------|
| MDC filter implementation | 3 |
| JSON layout configuration | 2 |
| Domain context integration | 5 |
| Async propagation | 2 |
| Testing | 2 |
| Documentation | 1 |

## Required Approvals

- [ ] Operations Team (Log format change)
- [ ] Security Review (PII in logs)
