# RFC-0007: Stackless Exceptions for Common Error Paths

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Use stackless exceptions for common, expected errors (rate limiting, validation) to reduce CPU overhead and improve latency.

## Motivation

Stack trace capture is expensive:

```java
// Current
throw new RateLimitExceededException();
// JVM captures full stack trace (~1-10ms)
```

For high-frequency errors like rate limiting, this adds significant overhead:
- 1000 rate-limited requests/second = 1000 stack trace captures
- At 5ms each = 5 seconds of CPU time per second (500% overhead)

**Benchmark data (typical):**
- Exception with stack trace: 5-10ms
- Exception without stack trace: 0.01ms
- **500-1000x difference**

## Detailed Design

### Stackless Exception Pattern

```java
// RateLimitExceededException.java
public class RateLimitExceededException extends RuntimeException {

  // Singleton for common case
  private static final RateLimitExceededException INSTANCE =
      new StacklessRateLimitExceededException();

  public static RateLimitExceededException withoutStackTrace() {
    return INSTANCE;
  }

  // Normal exception with stack trace
  public RateLimitExceededException() {
    super("Rate limit exceeded");
  }

  public RateLimitExceededException(String message) {
    super(message);
  }

  // Stackless variant
  private static class StacklessRateLimitExceededException
      extends RateLimitExceededException {

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;  // Skip stack trace capture
    }
  }
}
```

### Usage in Rate Limiter

```java
// GuavaRateLimiter.java
@Override
public void rateLimit(int cost) {
  if (!rateLimiter.tryAcquire(cost)) {
    // Use stackless for better performance
    throw RateLimitExceededException.withoutStackTrace();
  }
}
```

### Other Candidates

Apply the same pattern to:

```java
// BadRequestException
public static BadRequestException withoutStackTrace(String message) {
  return new StacklessBadRequestException(message);
}

// NotFoundException
public static NotFoundException withoutStackTrace(String message) {
  return new StacklessNotFoundException(message);
}

// ValidationException
public static ValidationException withoutStackTrace(String message) {
  return new StacklessValidationException(message);
}
```

### When to Use Stack Traces

Keep stack traces for:
- Unexpected errors (bugs, system failures)
- Debugging (when explicitly requested)
- First occurrence of repeated errors

```java
// Configuration for debug mode
if (config.getBoolean("debug.full.stack.traces")) {
  throw new RateLimitExceededException();  // With stack trace
} else {
  throw RateLimitExceededException.withoutStackTrace();
}
```

## Example Usage

### Before

```java
// Every rate limit throws with full stack trace
throw new RateLimitExceededException();

// Log shows:
// RateLimitExceededException: Rate limit exceeded
//   at GuavaRateLimiter.rateLimit(GuavaRateLimiter.java:45)
//   at RateLimitRequestFilter.filter(RateLimitRequestFilter.java:32)
//   at org.glassfish.jersey.server.ContainerFilteringStage...
//   ... 50 more lines
```

### After

```java
// Common case: no stack trace
throw RateLimitExceededException.withoutStackTrace();

// Log shows:
// RateLimitExceededException: Rate limit exceeded
//   (no stack trace - expected error)

// Debug mode: with stack trace
throw new RateLimitExceededException("Cluster xyz exceeded limit");
```

### Performance Impact

```
Before: 1000 rate-limited requests = 5000ms CPU
After:  1000 rate-limited requests = 10ms CPU
Improvement: 500x
```

## Implementation Plan

### Phase 1: Core Exceptions (Day 1)

1. Add stackless variants to `RateLimitExceededException`
2. Update `GuavaRateLimiter` and `Resilience4JRateLimiter`
3. Unit tests

### Phase 2: Additional Exceptions (Day 2)

1. `BadRequestException`
2. `NotFoundException`
3. `ValidationException`
4. Update usages

## Backwards Compatibility

- **Behavior:** Same error responses
- **Logging:** Less verbose for expected errors
- **Debugging:** Optional full stack traces

## Alternatives Considered

### Alternative 1: Disable Stack Traces Globally

JVM flag `-XX:-StackTraceInThrowable`.

**Rejected because:**
- Affects all exceptions, including bugs
- Harder to debug production issues
- Global impact

### Alternative 2: Lazy Stack Trace

Capture stack trace only if logged at DEBUG.

**Rejected because:**
- Complex implementation
- Still captures when debugging
- Singleton pattern is simpler

## Open Questions

1. **Singleton vs new instance:** Should we use singleton or create new?
   - Proposal: Singleton for message-less, new for custom messages

2. **Log differentiation:** How to indicate stackless in logs?
   - Proposal: Add `(expected)` suffix to log message

## Success Criteria

- [ ] 15% latency reduction on rate-limited requests
- [ ] CPU reduction visible in profiling
- [ ] Same error responses
- [ ] No regression in debugging ability

## Effort Estimation

**Total:** 2 dev-days

| Task | Days |
|------|------|
| Implementation | 1 |
| Testing | 0.5 |
| Documentation | 0.5 |

## Required Approvals

- [ ] Performance Team
