# RFC-0006: Cache Implementation Upgrade to Caffeine

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Replace Guava `LoadingCache` with Caffeine for rate limiter caching to improve concurrency and reduce lock contention.

## Motivation

Current rate limiter caching uses Guava:

```java
// ProduceRateLimiters.java
private final LoadingCache<String, RequestRateLimiter> countLimiters =
    CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiryMs, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<String, RequestRateLimiter>() {
          @Override
          public RequestRateLimiter load(String clusterId) {
            return rateLimiterFactory.create(...);
          }
        });
```

**Problems:**

1. **Lock contention** - `getUnchecked()` acquires segment lock on miss
2. **Stale reads** - Guava uses older concurrency model
3. **Memory overhead** - Higher memory per entry

**Caffeine benefits:**
- Near-optimal concurrency (based on ConcurrentLinkedHashMap)
- Lower memory footprint
- Better hit rates with TinyLFU eviction
- Active maintenance

## Detailed Design

### Cache Replacement

```java
// Before (Guava)
private final LoadingCache<String, RequestRateLimiter> countLimiters =
    CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiryMs, TimeUnit.MILLISECONDS)
        .build(cacheLoader);

// After (Caffeine)
private final LoadingCache<String, RequestRateLimiter> countLimiters =
    Caffeine.newBuilder()
        .expireAfterAccess(cacheExpiryMs, TimeUnit.MILLISECONDS)
        .maximumSize(10000)
        .recordStats()
        .build(key -> rateLimiterFactory.create(key));
```

### Dependency Addition

```xml
<!-- pom.xml -->
<dependency>
  <groupId>com.github.ben-manes.caffeine</groupId>
  <artifactId>caffeine</artifactId>
  <version>3.1.8</version>
</dependency>
```

### Metrics Integration

```java
// Caffeine provides built-in stats
CacheStats stats = countLimiters.stats();
cacheHitRate.set(stats.hitRate());
cacheMissRate.set(stats.missRate());
cacheEvictionCount.set(stats.evictionCount());
cacheLoadTime.set(stats.averageLoadPenalty());
```

### Configuration

```properties
# Rate limiter cache size
rate.limit.cache.maximum.size=10000

# Enable stats collection
rate.limit.cache.stats.enabled=true

# Eviction policy (already access-based)
rate.limit.cache.expiry.ms=3600000
```

## Example Usage

### API (No Changes)

```java
// Same interface
RequestRateLimiter limiter = countLimiters.get(clusterId);
limiter.rateLimit(cost);
```

### Metrics Access

```java
// Get cache statistics
CacheStats stats = rateLimiters.getCountLimiterStats();
log.info("Hit rate: {}, Miss rate: {}",
    stats.hitRate(), stats.missRate());
```

## Implementation Plan

### Phase 1: Dependency and Core (Day 1-2)

1. Add Caffeine dependency
2. Replace cache implementations
3. Update factory methods

### Phase 2: Metrics (Day 3)

1. Add stats recording
2. Expose metrics
3. Dashboard updates

### Phase 3: Testing (Day 4)

1. Unit tests
2. Concurrent access tests
3. Performance benchmarks

## Backwards Compatibility

- **API:** No changes
- **Configuration:** New optional properties
- **Behavior:** Same expiration semantics

## Alternatives Considered

### Alternative 1: Guava Tuning

Adjust Guava configuration (concurrency level, initial capacity).

**Rejected because:**
- Fundamental concurrency model unchanged
- Still segment-based locking
- Less efficient than Caffeine

### Alternative 2: ConcurrentHashMap

Simple map without caching features.

**Rejected because:**
- No automatic expiration
- No eviction policy
- Manual cleanup required

## Open Questions

1. **Eviction policy:** Should we use TinyLFU or LRU?
   - Proposal: Default (TinyLFU) for better hit rates

2. **Weak references:** Should entries be weakly referenced?
   - Proposal: No, rate limiters should be strongly held

## Success Criteria

- [ ] 20% reduction in cache access latency
- [ ] Zero lock contention under load
- [ ] Metrics visible in monitoring
- [ ] No behavior changes
- [ ] Same or lower memory usage

## Effort Estimation

**Total:** 4 dev-days

| Task | Days |
|------|------|
| Implementation | 2 |
| Testing | 1.5 |
| Documentation | 0.5 |

## Required Approvals

- [ ] Performance Team
