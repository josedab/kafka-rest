# RFC-0013: Connection Pooling for External Services

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Implement connection pooling for HTTP clients connecting to Schema Registry and other external services to prevent resource leaks and improve performance.

## Motivation

Currently, Schema Registry client creates connections without proper pooling:

```java
// SchemaRegistryClient uses default HTTP client
// No explicit connection pooling configured
```

**Problems:**

1. **Connection leaks** - Connections not properly closed
2. **No reuse** - New connection for every request
3. **High latency** - TCP handshake + TLS negotiation for each request
4. **Resource exhaustion** - Too many open connections

**Metrics from production:**
- TIME_WAIT connections: 1000+
- Connection establishment time: 50-100ms per request
- File descriptor usage: High

## Detailed Design

### HTTP Client Factory

```java
public class HttpClientFactory {

  public static CloseableHttpClient createPooledClient(
      KafkaRestConfig config) {

    // Connection pool configuration
    PoolingHttpClientConnectionManager connectionManager =
        new PoolingHttpClientConnectionManager();

    // Total connections across all routes
    connectionManager.setMaxTotal(
        config.getInt("http.client.max.connections.total"));

    // Connections per route (per host)
    connectionManager.setDefaultMaxPerRoute(
        config.getInt("http.client.max.connections.per.route"));

    // Connection timeouts
    connectionManager.setDefaultSocketConfig(
        SocketConfig.custom()
            .setSoTimeout(config.getInt("http.client.socket.timeout.ms"))
            .build());

    // Idle connection eviction
    connectionManager.setValidateAfterInactivity(
        config.getInt("http.client.validate.after.inactivity.ms"));

    // Request configuration
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(config.getInt("http.client.connect.timeout.ms"))
        .setConnectionRequestTimeout(
            config.getInt("http.client.connection.request.timeout.ms"))
        .setSocketTimeout(config.getInt("http.client.socket.timeout.ms"))
        .build();

    // HTTP client with connection pooling
    return HttpClients.custom()
        .setConnectionManager(connectionManager)
        .setDefaultRequestConfig(requestConfig)
        .setRetryHandler(new StandardHttpRequestRetryHandler(3, true))
        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
        .evictIdleConnections(30, TimeUnit.SECONDS)
        .evictExpiredConnections()
        .build();
  }
}
```

### Schema Registry Client Integration

```java
// SchemaRegistryModule.java
@Provides
@Singleton
public SchemaRegistryClient provideSchemaRegistryClient(
    KafkaRestConfig config) {

  String baseUrl = config.getString("schema.registry.url");
  if (baseUrl == null) {
    return Optional.empty();
  }

  // Create pooled HTTP client
  CloseableHttpClient httpClient = HttpClientFactory.createPooledClient(config);

  // Configure SR client with pooled HTTP client
  Map<String, Object> srConfig = new HashMap<>();
  srConfig.put("base.url", baseUrl);
  srConfig.put("max.schemas.per.subject", 1000);
  srConfig.put("http.client", httpClient);  // Use pooled client

  // Authentication
  if (config.getString("basic.auth.user.info") != null) {
    srConfig.put("basic.auth.credentials.source", "USER_INFO");
    srConfig.put("basic.auth.user.info",
        config.getString("basic.auth.user.info"));
  }

  return new CachedSchemaRegistryClient(
      Collections.singletonList(baseUrl),
      1000,  // Cache size
      srConfig);
}
```

### Connection Monitoring

```java
public class ConnectionPoolMonitor implements Runnable {

  private final PoolingHttpClientConnectionManager connectionManager;

  @Override
  public void run() {
    PoolStats totalStats = connectionManager.getTotalStats();

    // Expose metrics
    httpConnectionPoolSize.set(totalStats.getLeased());
    httpConnectionPoolAvailable.set(totalStats.getAvailable());
    httpConnectionPoolPending.set(totalStats.getPending());
    httpConnectionPoolMax.set(totalStats.getMax());

    // Log warnings
    if (totalStats.getLeased() > totalStats.getMax() * 0.9) {
      log.warn("HTTP connection pool nearly exhausted: {}/{}",
          totalStats.getLeased(), totalStats.getMax());
    }
  }
}

// Schedule monitoring
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(
    new ConnectionPoolMonitor(connectionManager),
    0, 30, TimeUnit.SECONDS);
```

### Configuration

```properties
# HTTP client connection pooling
http.client.enabled=true

# Total connections across all hosts
http.client.max.connections.total=200

# Connections per host (e.g., Schema Registry)
http.client.max.connections.per.route=50

# Connection timeouts
http.client.connect.timeout.ms=5000
http.client.connection.request.timeout.ms=1000
http.client.socket.timeout.ms=30000

# Idle connection management
http.client.validate.after.inactivity.ms=2000
http.client.idle.connection.eviction.interval.ms=30000

# Keep-alive strategy
http.client.keep.alive.duration.ms=60000

# Retry configuration
http.client.retry.count=3
http.client.retry.enabled=true
```

### Graceful Shutdown

```java
@Override
public void onShutdown() {
  try {
    // Close HTTP client
    if (httpClient != null) {
      httpClient.close();
      log.info("HTTP client closed");
    }

    // Close connection manager
    if (connectionManager != null) {
      connectionManager.close();
      log.info("Connection manager closed");
    }
  } catch (IOException e) {
    log.error("Error closing HTTP client", e);
  }
}
```

## Example Usage

### Before (No Pooling)

```
Request 1 → New connection → TCP handshake (20ms) → TLS (30ms) → Request (10ms) = 60ms
Request 2 → New connection → TCP handshake (20ms) → TLS (30ms) → Request (10ms) = 60ms
Request 3 → New connection → TCP handshake (20ms) → TLS (30ms) → Request (10ms) = 60ms

Total: 180ms for 3 requests
Connections: 3 new
TIME_WAIT: 3 connections
```

### After (With Pooling)

```
Request 1 → New connection → TCP handshake (20ms) → TLS (30ms) → Request (10ms) = 60ms
Request 2 → Reuse connection → Request (10ms) = 10ms
Request 3 → Reuse connection → Request (10ms) = 10ms

Total: 80ms for 3 requests (55% faster)
Connections: 1 reused
TIME_WAIT: 0 (connection kept alive)
```

### Metrics Dashboard

```
HTTP Connection Pool Status:
- Total connections: 45 / 200
- Available: 35
- Leased: 10
- Pending: 0

Per-Route Stats:
- schema-registry:8081: 8 / 50
- kafka-broker-1:9092: 12 / 50
- kafka-broker-2:9092: 10 / 50
```

## Implementation Plan

### Phase 1: HTTP Client Setup (Week 1)

1. Add Apache HttpClient dependency
2. Create `HttpClientFactory`
3. Configure connection pooling

### Phase 2: Integration (Week 2)

1. Update Schema Registry client
2. Connection monitoring
3. Graceful shutdown

### Phase 3: Testing (Week 2)

1. Load testing
2. Connection leak detection
3. Performance benchmarks

## Backwards Compatibility

- **Default:** Connection pooling enabled
- **Transparent:** No API changes
- **Performance:** Only improvements

## Alternatives Considered

### Alternative 1: OkHttp

Use OkHttp instead of Apache HttpClient.

**Rejected because:**
- Apache HttpClient better integration with existing code
- More mature connection pooling
- Better monitoring

### Alternative 2: Netty

Use Netty for async HTTP.

**Rejected because:**
- Overkill for current needs
- More complex
- Sync client sufficient

## Open Questions

1. **Connection limits:** What are optimal values?
   - Proposal: 200 total, 50 per route (tunable)

2. **Keep-alive duration:** How long to keep connections?
   - Proposal: 60 seconds (configurable)

## Success Criteria

- [ ] Zero connection leaks
- [ ] 50% reduction in connection establishment time
- [ ] 90% connection reuse rate
- [ ] No TIME_WAIT buildup

## Effort Estimation

**Total:** 10 dev-days

| Task | Days |
|------|------|
| HTTP client implementation | 3 |
| SR client integration | 2 |
| Monitoring | 2 |
| Testing | 3 |

## Required Approvals

- [ ] Architecture Review
- [ ] Performance Team
