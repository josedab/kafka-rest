# RFC-0009: Distributed Tracing with OpenTelemetry

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Integrate OpenTelemetry for distributed tracing across REST Proxy, Kafka, and downstream services to dramatically reduce debugging time and improve observability.

## Motivation

Currently, there is no distributed tracing in REST Proxy:

**Debugging problems today:**
- Request fails → Check REST Proxy logs
- Not enough info → Check Kafka broker logs
- Still unclear → Check Schema Registry logs
- Check application logs
- **Total time: 2-3 hours per issue**

**With distributed tracing:**
- Single trace ID shows complete request path
- See exact latency breakdown
- Identify bottleneck immediately
- **Total time: < 5 minutes**

**Real-world scenarios:**
1. **Slow produce requests** - Is it schema resolution, serialization, or Kafka?
2. **Consumer lag investigation** - Where is the bottleneck?
3. **Multi-service transactions** - Track request across all systems
4. **Performance regression** - Identify which component slowed down

## Detailed Design

### OpenTelemetry Integration

```java
// OpenTelemetryModule.java
public class OpenTelemetryModule extends AbstractBinder {

  @Override
  protected void configure() {
    bind(OpenTelemetryProvider.class)
        .to(OpenTelemetry.class)
        .in(Singleton.class);

    bind(TracingFilter.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  public OpenTelemetry provideOpenTelemetry(KafkaRestConfig config) {
    String serviceName = "kafka-rest-proxy";
    String endpoint = config.getString("otel.exporter.otlp.endpoint");

    return AutoConfiguredOpenTelemetrySdk.builder()
        .addResourceCustomizer((resource, configProperties) ->
            resource.merge(Resource.create(Attributes.of(
                ResourceAttributes.SERVICE_NAME, serviceName,
                ResourceAttributes.SERVICE_VERSION, getVersion(),
                ResourceAttributes.DEPLOYMENT_ENVIRONMENT, getEnvironment()))))
        .build()
        .getOpenTelemetrySdk();
  }
}
```

### Tracing Filter

```java
@Provider
@PreMatching
@Priority(Priorities.HEADER_DECORATOR)
public class TracingFilter implements ContainerRequestFilter,
                                      ContainerResponseFilter {

  private final Tracer tracer;
  private final TextMapPropagator propagator;

  @Inject
  public TracingFilter(OpenTelemetry openTelemetry) {
    this.tracer = openTelemetry.getTracer("kafka-rest-proxy");
    this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    // Extract trace context from headers
    Context extractedContext = propagator.extract(
        Context.current(),
        requestContext,
        new HeaderExtractor());

    // Start span
    Span span = tracer.spanBuilder(getSpanName(requestContext))
        .setParent(extractedContext)
        .setSpanKind(SpanKind.SERVER)
        .startSpan();

    // Add attributes
    span.setAttribute("http.method", requestContext.getMethod());
    span.setAttribute("http.url", requestContext.getUriInfo().getRequestUri().toString());
    span.setAttribute("http.route", getRoute(requestContext));

    // Store span in request context
    requestContext.setProperty("otel.span", span);
    requestContext.setProperty("otel.context",
        extractedContext.with(span));
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
                    ContainerResponseContext responseContext) {
    Span span = (Span) requestContext.getProperty("otel.span");
    if (span != null) {
      span.setAttribute("http.status_code", responseContext.getStatus());

      if (responseContext.getStatus() >= 400) {
        span.setStatus(StatusCode.ERROR);
      }

      span.end();
    }
  }
}
```

### Instrumented Produce Controller

```java
public class ProduceControllerImpl implements ProduceController {

  private final Tracer tracer;

  @Override
  public CompletableFuture<ProduceResult> produce(
      String clusterId,
      String topicName,
      /* ... */) {

    // Create child span
    Span span = tracer.spanBuilder("kafka.produce")
        .setSpanKind(SpanKind.CLIENT)
        .setAttribute("messaging.system", "kafka")
        .setAttribute("messaging.destination", topicName)
        .setAttribute("messaging.kafka.cluster_id", clusterId)
        .startSpan();

    try (Scope scope = span.makeCurrent()) {
      Producer<byte[], byte[]> producer = producerProvider.get();

      ProducerRecord<byte[], byte[]> record = createRecord(/* ... */);

      // Inject trace context into Kafka headers
      propagator.inject(Context.current(), record.headers(),
          new KafkaHeadersSetter());

      CompletableFuture<ProduceResult> future = new CompletableFuture<>();

      producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          span.recordException(exception);
          span.setStatus(StatusCode.ERROR);
          future.completeExceptionally(exception);
        } else {
          span.setAttribute("messaging.kafka.partition", metadata.partition());
          span.setAttribute("messaging.kafka.offset", metadata.offset());
          span.setStatus(StatusCode.OK);
          future.complete(toProduceResult(metadata));
        }
        span.end();
      });

      return future;
    }
  }
}
```

### Schema Manager Instrumentation

```java
public class SchemaManagerImpl implements SchemaManager {

  private final Tracer tracer;

  @Override
  public RegisteredSchema getSchema(/* ... */) {
    Span span = tracer.spanBuilder("schema_registry.get_schema")
        .setSpanKind(SpanKind.CLIENT)
        .setAttribute("schema.subject", subject.orElse("unknown"))
        .startSpan();

    try (Scope scope = span.makeCurrent()) {
      RegisteredSchema schema = schemaRegistryClient.getSchema(/* ... */);
      span.setAttribute("schema.id", schema.getId());
      span.setAttribute("schema.version", schema.getVersion());
      return schema;
    } catch (Exception e) {
      span.recordException(e);
      span.setStatus(StatusCode.ERROR);
      throw e;
    } finally {
      span.end();
    }
  }
}
```

### Configuration

```properties
# OpenTelemetry configuration
otel.traces.enabled=true
otel.metrics.enabled=true

# OTLP exporter (e.g., Jaeger, Tempo)
otel.exporter.otlp.endpoint=http://tempo:4317
otel.exporter.otlp.protocol=grpc

# Sampling (1.0 = 100%, 0.1 = 10%)
otel.traces.sampler=parentbased_traceidratio
otel.traces.sampler.arg=1.0

# Service identification
otel.service.name=kafka-rest-proxy
otel.resource.attributes=deployment.environment=production,service.version=8.2.0

# Propagators
otel.propagators=tracecontext,baggage
```

## Example Usage

### Trace Visualization

```
Trace ID: 1234567890abcdef
Duration: 245ms

├─ HTTP POST /v3/clusters/abc/topics/orders/records [200ms]
│  ├─ rate_limit.check [2ms]
│  ├─ schema_registry.get_schema [85ms] ⚠️ SLOW
│  │  └─ http.get /subjects/orders-value/versions/latest [83ms]
│  ├─ serialize.avro [8ms]
│  ├─ kafka.produce [105ms]
│  │  └─ kafka.send topic=orders partition=3 offset=12345 [103ms]
│  └─ response.format [5ms]
```

**Insight:** Schema Registry call is the bottleneck (85ms of 245ms)

### Trace Attributes

```json
{
  "trace_id": "1234567890abcdef",
  "span_id": "fedcba0987654321",
  "parent_span_id": "abcdef1234567890",
  "name": "kafka.produce",
  "kind": "CLIENT",
  "start_time": "2025-11-19T10:30:00.000Z",
  "end_time": "2025-11-19T10:30:00.105Z",
  "duration_ms": 105,
  "attributes": {
    "messaging.system": "kafka",
    "messaging.destination": "orders",
    "messaging.kafka.cluster_id": "abc",
    "messaging.kafka.partition": 3,
    "messaging.kafka.offset": 12345,
    "http.status_code": 200
  },
  "status": "OK"
}
```

### End-to-End Trace

```
┌─────────────┐     ┌──────────────┐     ┌────────┐     ┌──────┐
│   Client    │────▶│  REST Proxy  │────▶│ Schema │────▶│Kafka │
└─────────────┘     └──────────────┘     │Registry│     └──────┘
                                         └────────┘
     5ms                200ms               85ms        103ms

Trace shows:
- Client → REST Proxy: 5ms (network)
- REST Proxy processing: 200ms
  - Schema Registry: 85ms (bottleneck!)
  - Serialization: 8ms
  - Kafka produce: 103ms
  - Other: 4ms
```

## Implementation Plan

### Phase 1: Core Integration (Week 1-2)

1. Add OpenTelemetry dependencies
2. Create `OpenTelemetryModule`
3. Implement `TracingFilter`

### Phase 2: Instrumentation (Week 2-3)

1. Instrument `ProduceController`
2. Instrument `SchemaManager`
3. Instrument `ConsumerManager`
4. Instrument `Admin` operations

### Phase 3: Testing and Rollout (Week 3-4)

1. Integration with Jaeger/Tempo
2. Performance testing (overhead < 1%)
3. Documentation and runbooks

## Backwards Compatibility

- **Default:** Disabled (`otel.traces.enabled=false`)
- **Opt-in:** Enable via configuration
- **Zero overhead:** When disabled, no performance impact

## Alternatives Considered

### Alternative 1: Custom Logging with Correlation IDs

Add correlation IDs to all log statements.

**Rejected because:**
- Requires manual log correlation
- No visualization
- Misses timing information
- Labor-intensive

### Alternative 2: Kafka Consumer Interceptors Only

Trace only within Kafka.

**Rejected because:**
- Doesn't cover Schema Registry
- Doesn't cover HTTP layer
- Incomplete picture

## Open Questions

1. **Sampling rate:** 100% or lower in production?
   - Proposal: 100% initially, adjust based on volume

2. **Span granularity:** How detailed should spans be?
   - Proposal: One span per major operation

3. **Baggage propagation:** Should we propagate business context?
   - Proposal: Yes, for request_id, user_id, etc.

## Success Criteria

- [ ] < 1% performance overhead
- [ ] 100% trace coverage for produce/consume operations
- [ ] MTTR reduced from 2-3 hours to < 5 minutes
- [ ] Visualization in Jaeger/Grafana
- [ ] Integration with existing monitoring

## Effort Estimation

**Total:** 20 dev-days

| Task | Days |
|------|------|
| Design and review | 2 |
| OpenTelemetry integration | 4 |
| Produce path instrumentation | 3 |
| Consume path instrumentation | 3 |
| Schema Registry instrumentation | 2 |
| Testing | 4 |
| Documentation | 2 |

## Required Approvals

- [ ] Architecture Review
- [ ] Operations Team (New infrastructure: Jaeger/Tempo)
- [ ] Security Review (Data in traces)
