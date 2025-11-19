# RFC-0002: Distributed Rate Limiting with Redis

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Implement distributed rate limiting using Redis to coordinate rate limits across multiple REST Proxy instances.

## Motivation

Current rate limiting is memory-only per instance:

```java
// GuavaRateLimiter.java
private final RateLimiter rateLimiter = RateLimiter.create(permitsPerSecond);
```

This causes problems in scaled deployments:

1. **No coordination** - Each instance has its own limits
2. **Unfair distribution** - Load balancer distribution affects effective limits
3. **Inconsistent enforcement** - Clients can exceed limits by hitting different instances
4. **No persistence** - Limits reset on restart

**Example problem:**
- Config: 100 requests/second per cluster
- 3 REST Proxy instances
- Actual limit: 300 requests/second (3 x 100)

## Detailed Design

### Architecture

```
┌─────────────────┐     ┌─────────────────┐
│  REST Proxy 1   │     │  REST Proxy 2   │
│  ┌───────────┐  │     │  ┌───────────┐  │
│  │Redis Rate │  │     │  │Redis Rate │  │
│  │ Limiter   │  │     │  │ Limiter   │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │
└────────┼────────┘     └────────┼────────┘
         │                       │
         └───────────┬───────────┘
                     │
              ┌──────▼──────┐
              │    Redis    │
              │   Cluster   │
              └─────────────┘
```

### Redis Rate Limiter Implementation

```java
public class RedisRateLimiter implements RateLimitBackend {

  private final RedisClient redisClient;
  private final String keyPrefix;
  private final int permitsPerSecond;
  private final int windowSizeSeconds;

  // Lua script for atomic rate limiting
  private static final String RATE_LIMIT_SCRIPT = """
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local cost = tonumber(ARGV[3])
    local now = tonumber(ARGV[4])

    -- Remove old entries
    redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window * 1000)

    -- Count current entries
    local count = redis.call('ZCARD', key)

    if count + cost <= limit then
      -- Add new entries
      for i = 1, cost do
        redis.call('ZADD', key, now, now .. ':' .. i)
      end
      redis.call('EXPIRE', key, window * 2)
      return 1  -- Allowed
    else
      return 0  -- Rejected
    end
    """;

  @Override
  public void rateLimit(int cost) {
    String key = keyPrefix + ":ratelimit";
    long now = System.currentTimeMillis();

    Long result = redisClient.eval(
        RATE_LIMIT_SCRIPT,
        List.of(key),
        List.of(
            String.valueOf(permitsPerSecond * windowSizeSeconds),
            String.valueOf(windowSizeSeconds),
            String.valueOf(cost),
            String.valueOf(now)));

    if (result == 0) {
      throw new RateLimitExceededException();
    }
  }
}
```

### Configuration

```properties
# Enable distributed rate limiting
rate.limit.backend=redis

# Redis connection
rate.limit.redis.host=localhost
rate.limit.redis.port=6379
rate.limit.redis.password=secret
rate.limit.redis.ssl.enabled=true

# Redis cluster mode
rate.limit.redis.cluster.enabled=true
rate.limit.redis.cluster.nodes=host1:6379,host2:6379,host3:6379

# Sliding window configuration
rate.limit.redis.window.seconds=1

# Fallback on Redis failure
rate.limit.redis.fallback.enabled=true
rate.limit.redis.fallback.backend=guava
```

### Module Integration

```java
// RateLimitModule.java
@Override
protected void configure() {
  String backend = config.getString("rate.limit.backend");

  switch (backend) {
    case "redis":
      bind(RedisRateLimiter.class).to(RateLimitBackend.class);
      bind(RedisClient.class).toProvider(RedisClientProvider.class);
      break;
    case "guava":
      bind(GuavaRateLimiter.class).to(RateLimitBackend.class);
      break;
    case "resilience4j":
      bind(Resilience4JRateLimiter.class).to(RateLimitBackend.class);
      break;
  }
}
```

### Fallback Handling

```java
public class ResilientRedisRateLimiter implements RateLimitBackend {

  private final RedisRateLimiter redisLimiter;
  private final RateLimitBackend fallbackLimiter;
  private final CircuitBreaker circuitBreaker;

  @Override
  public void rateLimit(int cost) {
    try {
      circuitBreaker.executeRunnable(() ->
          redisLimiter.rateLimit(cost));
    } catch (Exception e) {
      log.warn("Redis rate limiter failed, using fallback", e);
      fallbackLimiter.rateLimit(cost);
      redisFailures.increment();
    }
  }
}
```

## Example Usage

### Before (Per-Instance)

Instance 1: 0-100 requests allowed
Instance 2: 0-100 requests allowed
Instance 3: 0-100 requests allowed
**Total: 300 requests (3x configured)**

### After (Distributed)

Instance 1: 0-33 requests allowed
Instance 2: 34-66 requests allowed
Instance 3: 67-100 requests allowed
**Total: 100 requests (as configured)**

## Implementation Plan

### Phase 1: Core Redis Integration (Week 1-2)

1. Add Redis client dependency (Lettuce)
2. Implement `RedisRateLimiter`
3. Lua script for atomic operations

### Phase 2: Fault Tolerance (Week 3)

1. Circuit breaker integration
2. Fallback to local limiter
3. Health checks

### Phase 3: Production Readiness (Week 4-5)

1. SSL/TLS support
2. Redis Cluster support
3. Metrics and monitoring

### Phase 4: Testing and Documentation (Week 6)

1. Integration tests
2. Performance benchmarks
3. Runbook documentation

## Backwards Compatibility

- **Default:** `rate.limit.backend=guava` (unchanged)
- **Migration:** Opt-in via configuration
- **API:** No changes

### Rollback

```properties
# Immediate rollback
rate.limit.backend=guava
```

## Alternatives Considered

### Alternative 1: Hazelcast

Distributed map with local caching.

**Rejected because:**
- Additional infrastructure
- JVM memory overhead
- More complex operations

### Alternative 2: Database-based

Store rate limit state in PostgreSQL/MySQL.

**Rejected because:**
- Higher latency
- Database becomes critical path
- Scaling limitations

### Alternative 3: Token Bucket in Redis

Pre-allocated tokens with decrement.

**Rejected because:**
- Sliding window provides better fairness
- Token replenishment complexity

## Open Questions

1. **Redis persistence:** AOF or RDB for rate limit data?
   - Proposal: Neither (ephemeral, acceptable loss on restart)

2. **Clock skew:** How to handle time differences between instances?
   - Proposal: Use Redis server time via TIME command

3. **Multi-region:** How to handle geo-distributed deployments?
   - Proposal: Future RFC for multi-region rate limiting

4. **Cost allocation:** How to charge multiple-cost operations?
   - Proposal: Same as current (cost parameter in Lua script)

## Success Criteria

- [ ] Rate limits enforced across all instances
- [ ] < 5ms added latency for rate limit check
- [ ] Graceful degradation on Redis failure
- [ ] No data loss on instance restart
- [ ] Metrics for Redis operations

## Effort Estimation

**Total:** 30 dev-days

| Task | Days |
|------|------|
| Design and review | 3 |
| Redis client integration | 5 |
| Rate limiter implementation | 5 |
| Fallback and circuit breaker | 4 |
| Cluster and SSL support | 4 |
| Testing | 6 |
| Documentation | 3 |

## Required Approvals

- [ ] Architecture Review
- [ ] Operations (Redis infrastructure)
- [ ] Security Review (Redis authentication)
