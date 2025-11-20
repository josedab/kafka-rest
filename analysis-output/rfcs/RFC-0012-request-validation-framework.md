# RFC-0012: Request Input Validation Framework

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-11-19
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Summary

Implement comprehensive request input validation to prevent resource exhaustion attacks, improve security, and provide better error messages.

## Motivation

Current validation is minimal and inconsistent:

```java
// ProduceAction.java - Limited validation
public void produce(
    @PathParam("topicName") String topicName,
    MappingIterator<ProduceRequest> requests) {
  // No validation on:
  // - Topic name format
  // - Request count
  // - Message size
  // - Header count/size
}
```

**Security vulnerabilities:**

1. **OOM attacks** - Send 1 million records in batch
2. **CPU exhaustion** - Send malformed JSON that triggers expensive parsing
3. **Buffer overflow** - Send huge message payloads
4. **Resource exhaustion** - Create thousands of consumers

**Real-world attacks:**
- Malicious client sends 10 MB message → OOM
- Batch with 100,000 records → CPU exhaustion
- Topic name with special chars → injection attacks

## Detailed Design

### Validation Framework

```java
@Target({METHOD, FIELD, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {})
public @interface ValidTopicName {
  String message() default "Invalid topic name";
  Class<?>[] groups() default {};
  Class<? extends Payload>[] payload() default {};

  // Kafka topic naming rules
  String pattern() default "^[a-zA-Z0-9._-]+$";
  int maxLength() default 249;
}

@Target({METHOD, FIELD, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {})
public @interface ValidBatchSize {
  String message() default "Batch size exceeds maximum";
  int max() default 50;
}

@Target({METHOD, FIELD, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {})
public @interface ValidMessageSize {
  String message() default "Message size exceeds maximum";
  long maxBytes() default 10485760;  // 10 MB
}
```

### Validated Endpoints

```java
// ProduceAction.java (enhanced)
@POST
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public void produce(
    @Suspended AsyncResponse asyncResponse,
    @PathParam("clusterId") String clusterId,
    @PathParam("topicName")
    @ValidTopicName String topicName,
    @Valid
    @ValidatedInputStream(
        maxSize = "10MB",
        maxRecords = 50)
    MappingIterator<ProduceRequest> requests) {
  // Validation happens before this code executes
}

// ProduceRequest.java (enhanced)
public abstract class ProduceRequest {

  @ValidMessageSize(maxBytes = 10485760)
  public abstract Optional<ProduceRequestData> getValue();

  @ValidMessageSize(maxBytes = 1048576)
  public abstract Optional<ProduceRequestData> getKey();

  @Size(max = 100, message = "Too many headers")
  public abstract Multimap<String, Optional<ByteString>> getHeaders();
}
```

### Custom Validators

```java
public class TopicNameValidator implements
    ConstraintValidator<ValidTopicName, String> {

  private Pattern pattern;
  private int maxLength;

  @Override
  public void initialize(ValidTopicName annotation) {
    this.pattern = Pattern.compile(annotation.pattern());
    this.maxLength = annotation.maxLength();
  }

  @Override
  public boolean isValid(String value, ConstraintValidatorContext context) {
    if (value == null) {
      return true;  // @NotNull handles null
    }

    if (value.length() > maxLength) {
      context.disableDefaultConstraintViolation();
      context.buildConstraintViolationWithTemplate(
          "Topic name exceeds maximum length of " + maxLength)
          .addConstraintViolation();
      return false;
    }

    if (!pattern.matcher(value).matches()) {
      context.disableDefaultConstraintViolation();
      context.buildConstraintViolationWithTemplate(
          "Topic name contains invalid characters. "
          + "Only alphanumeric, dots, underscores, and hyphens allowed")
          .addConstraintViolation();
      return false;
    }

    // Reserved names
    if (value.startsWith("__")) {
      context.disableDefaultConstraintViolation();
      context.buildConstraintViolationWithTemplate(
          "Topic names starting with '__' are reserved")
          .addConstraintViolation();
      return false;
    }

    return true;
  }
}
```

### Streaming Validation

```java
public class ValidatedInputStream {

  private final MappingIterator<ProduceRequest> delegate;
  private final long maxSize;
  private final int maxRecords;
  private long bytesRead = 0;
  private int recordsRead = 0;

  @Override
  public boolean hasNext() {
    if (recordsRead >= maxRecords) {
      throw new BadRequestException(
          "Batch size exceeds maximum of " + maxRecords + " records");
    }
    return delegate.hasNext();
  }

  @Override
  public ProduceRequest next() {
    ProduceRequest request = delegate.next();
    recordsRead++;

    // Track size
    long recordSize = estimateSize(request);
    bytesRead += recordSize;

    if (bytesRead > maxSize) {
      throw new BadRequestException(
          String.format("Request size exceeds maximum of %d bytes. "
              + "Current size: %d bytes", maxSize, bytesRead));
    }

    // Validate individual record
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<ProduceRequest>> violations =
        validator.validate(request);

    if (!violations.isEmpty()) {
      String errors = violations.stream()
          .map(ConstraintViolation::getMessage)
          .collect(Collectors.joining(", "));
      throw new BadRequestException("Validation failed: " + errors);
    }

    return request;
  }
}
```

### Configuration

```properties
# Request validation
validation.enabled=true

# Topic name validation
validation.topic.name.pattern=^[a-zA-Z0-9._-]+$
validation.topic.name.max.length=249

# Message size limits
validation.message.max.size.bytes=10485760
validation.key.max.size.bytes=1048576

# Batch limits
validation.batch.max.records=50
validation.batch.max.size.bytes=10485760

# Header limits
validation.headers.max.count=100
validation.headers.max.size.bytes=8192

# Consumer limits
validation.consumer.max.instances.per.group=100
validation.consumer.max.poll.records=500
```

### Error Response

```json
{
  "error_code": 40001,
  "message": "Validation failed",
  "violations": [
    {
      "field": "topicName",
      "value": "my topic",
      "message": "Topic name contains invalid characters. Only alphanumeric, dots, underscores, and hyphens allowed"
    },
    {
      "field": "value",
      "value": "<truncated>",
      "message": "Message size exceeds maximum of 10485760 bytes"
    }
  ]
}
```

## Example Usage

### Before (No Validation)

```bash
# Malicious request
curl -X POST http://localhost:8082/v3/clusters/abc/topics/my%20topic/records \
  -d '{"value":{"type":"STRING","data":"'$(python -c 'print("A"*100000000)')'"}}}'

# Result: OOM, server crash
```

### After (With Validation)

```bash
# Same malicious request
curl -X POST http://localhost:8082/v3/clusters/abc/topics/my%20topic/records \
  -d '{"value":{"type":"STRING","data":"'$(python -c 'print("A"*100000000)')'"}}}'

# Result: HTTP 400
{
  "error_code": 40001,
  "message": "Validation failed",
  "violations": [
    {
      "field": "topicName",
      "message": "Topic name contains invalid characters"
    },
    {
      "field": "value",
      "message": "Message size exceeds maximum of 10485760 bytes"
    }
  ]
}
```

## Implementation Plan

### Phase 1: Framework Setup (Week 1)

1. Add Bean Validation dependency
2. Create custom annotations
3. Configure validation

### Phase 2: Validators (Week 2)

1. Topic name validator
2. Message size validator
3. Batch size validator
4. Streaming validator

### Phase 3: Integration (Week 2)

1. Apply to all endpoints
2. Error response formatting
3. Testing

## Backwards Compatibility

- **Default:** Validation enabled
- **Opt-out:** `validation.enabled=false` (not recommended)
- **Graceful:** Existing valid requests unaffected

## Alternatives Considered

### Alternative 1: Manual Validation

Add validation code in each endpoint.

**Rejected because:**
- Code duplication
- Easy to forget
- Inconsistent error messages

### Alternative 2: API Gateway Validation

Validate at API gateway level.

**Rejected because:**
- Gateway doesn't understand Kafka semantics
- Can't validate against Kafka limits
- Still need internal validation

## Open Questions

1. **Custom validators:** Should users be able to add custom validators?
   - Proposal: Yes, via RestResourceExtension

2. **Async validation:** Should we validate asynchronously?
   - Proposal: No, synchronous is simpler and fast enough

## Success Criteria

- [ ] Zero OOM attacks
- [ ] All malformed requests rejected with clear errors
- [ ] < 1ms validation overhead
- [ ] 100% endpoint coverage

## Effort Estimation

**Total:** 10 dev-days

| Task | Days |
|------|------|
| Framework setup | 2 |
| Validator implementation | 4 |
| Integration | 2 |
| Testing | 2 |

## Required Approvals

- [ ] Security Review
- [ ] Architecture Review
