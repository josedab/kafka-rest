# Design Patterns and Best Practices in Kafka REST Proxy

**Series:** Kafka REST Proxy Deep Dive | **Part 3 of 6**
**Analysis Commit:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## What You'll Learn

- Design patterns employed throughout the codebase
- Error handling strategies and why they work
- Configuration management approaches
- Lessons you can apply to your own projects

## Introduction

Good software is built on good patterns. Kafka REST Proxy has evolved over years of production use, accumulating patterns that solve real problems. In this post, we'll extract these patterns and understand why they were chosen.

## Factory and Provider Patterns

### The Problem

How do you create objects with complex dependencies while keeping code testable?

### The Solution

REST Proxy uses both Factory and Provider patterns extensively:

```java
// Provider pattern - Lazy injection
// TopicsResource.java
public class TopicsResource {
  @Inject
  public TopicsResource(Provider<TopicManager> topicManager) {
    this.topicManager = topicManager;
  }

  @GET
  public void listTopics(...) {
    TopicManager manager = topicManager.get(); // Resolved per-request
    // ...
  }
}
```

```java
// Factory pattern - Complex creation
// KafkaModule.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/backends/kafka/KafkaModule.java
public static class ProducerFactory implements Factory<Producer<byte[], byte[]>> {
  @Override
  public Producer<byte[], byte[]> provide() {
    return new KafkaProducer<>(config.getProducerConfigs());
  }

  @Override
  public void dispose(Producer<byte[], byte[]> producer) {
    producer.close();
  }
}
```

### Why It Works

1. **Provider** delays instantiation until needed
2. **Factory** encapsulates creation and disposal logic
3. **Both** enable mocking in tests

### Lesson for Your Projects

Use `Provider<T>` when you need:
- Request-scoped objects in singletons
- Lazy initialization
- Optional dependencies

Use Factory when you need:
- Complex creation logic
- Resource cleanup
- Configurable instantiation

## Strategy Pattern

### The Problem

How do you support multiple algorithms (rate limiters, serializers) without conditional logic?

### The Solution

Define interfaces and let configuration choose implementations:

```java
// RateLimitBackend.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/ratelimit/RateLimitBackend.java
public interface RateLimitBackend {
  void rateLimit(int cost);
}

// GuavaRateLimiter.java
public class GuavaRateLimiter implements RateLimitBackend {
  private final RateLimiter rateLimiter;

  @Override
  public void rateLimit(int cost) {
    if (!rateLimiter.tryAcquire(cost)) {
      throw new RateLimitExceededException();
    }
  }
}

// Resilience4JRateLimiter.java
public class Resilience4JRateLimiter implements RateLimitBackend {
  // Different implementation
}
```

Configuration selects the strategy:

```java
// RateLimitModule.java
String backend = config.getString("rate.limit.backend");
switch (backend) {
  case "guava":
    bind(GuavaRateLimiter.class).to(RateLimitBackend.class);
    break;
  case "resilience4j":
    bind(Resilience4JRateLimiter.class).to(RateLimitBackend.class);
    break;
}
```

### Lesson for Your Projects

Use Strategy when:
- You have multiple algorithms for the same operation
- Configuration should select the algorithm
- You want to add new algorithms without modifying existing code

## Null Object Pattern

### The Problem

How do you handle optional features (like Schema Registry) without null checks everywhere?

### The Solution

Provide a "null object" that does nothing but satisfies the interface:

```java
// SchemaManagerThrowing.java
// When Schema Registry is not configured
public class SchemaManagerThrowing implements SchemaManager {
  @Override
  public RegisteredSchema getSchema(...) {
    throw new BadRequestException("Schema Registry is not configured");
  }
}

// SchemaManagerImpl.java
// When Schema Registry is configured
public class SchemaManagerImpl implements SchemaManager {
  @Override
  public RegisteredSchema getSchema(...) {
    // Actual implementation
  }
}
```

The module decides which to use:

```java
// ControllersModule.java
if (schemaRegistryConfigured) {
  bind(SchemaManagerImpl.class).to(SchemaManager.class);
} else {
  bind(SchemaManagerThrowing.class).to(SchemaManager.class);
}
```

### Lesson for Your Projects

Use Null Object when:
- A feature is optional
- You want to avoid null checks
- Disabled behavior should fail clearly

## Feature Pattern (JAX-RS)

### The Problem

How do you organize related JAX-RS components (resources, filters, providers)?

### The Solution

Jersey's Feature pattern groups related registrations:

```java
// V3ResourcesFeature.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/V3ResourcesFeature.java
public class V3ResourcesFeature implements Feature {
  @Override
  public boolean configure(FeatureContext context) {
    // Register all V3 resources
    context.register(ClustersResource.class);
    context.register(TopicsResource.class);
    context.register(BrokersResource.class);
    context.register(ProduceAction.class);
    // ... 31 total

    // Register V3-specific modules
    context.register(new V3ResourcesModule());

    return true;
  }
}
```

### Lesson for Your Projects

Use Feature when:
- You have groups of related JAX-RS components
- Features should be enabled/disabled together
- You want to organize registration code

## Error Handling Strategy

### The Problem

How do you handle errors consistently across API versions with different response formats?

### The Solution

A delegating exception mapper routes to version-specific handlers:

```java
// ExceptionsModule.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/ExceptionsModule.java
public class DelegatingExceptionMapper implements ExceptionMapper<Throwable> {
  @Override
  public Response toResponse(Throwable exception) {
    String path = uriInfo.getPath();

    if (path.startsWith("v3")) {
      return v3Mapper.toResponse(exception);
    } else {
      return v2Mapper.toResponse(exception);
    }
  }
}
```

Each mapper produces version-appropriate responses:

```java
// V3ExceptionMapper.java
@Override
public Response toResponse(Throwable exception) {
  ErrorResponse error = ErrorResponse.create(
      getErrorCode(exception),
      exception.getMessage());

  return Response.status(getStatus(exception))
      .entity(error)
      .type(MediaType.APPLICATION_JSON)
      .build();
}
```

### Error Code Design

Error codes follow a pattern for easy identification:

```java
// Errors.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/Errors.java
public static final int TOPIC_NOT_FOUND_ERROR_CODE = 40401;  // 404 + 01
public static final int PARTITION_NOT_FOUND_ERROR_CODE = 40402;  // 404 + 02
public static final int CONSUMER_NOT_FOUND_ERROR_CODE = 40403;  // 404 + 03

// Pattern: HTTP_STATUS + SPECIFIC_CODE
```

### Lesson for Your Projects

1. Use delegating handlers for multi-version APIs
2. Design error codes with patterns (HTTP status + specific)
3. Include error codes in logs for easy debugging

## Configuration Management

### The Problem

How do you manage 100+ configuration options without chaos?

### The Solution

Centralized configuration with Apache Kafka's ConfigDef:

```java
// KafkaRestConfig.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/KafkaRestConfig.java
public class KafkaRestConfig extends RestConfig {

  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  private static final String BOOTSTRAP_SERVERS_DOC =
      "A list of Kafka brokers to connect to.";
  private static final String BOOTSTRAP_SERVERS_DEFAULT = "";

  private static final ConfigDef config = baseConfigDef()
      .define(
          BOOTSTRAP_SERVERS_CONFIG,
          Type.STRING,
          BOOTSTRAP_SERVERS_DEFAULT,
          Importance.HIGH,
          BOOTSTRAP_SERVERS_DOC)
      // ... 100+ more definitions
      ;
}
```

### ConfigModule Binding

Individual config values are bound for injection:

```java
// ConfigModule.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/config/ConfigModule.java
bind(String.class)
    .annotatedWith(HostNameConfig.class)
    .toInstance(config.getString(KafkaRestConfig.HOST_NAME_CONFIG));

bind(Boolean.class)
    .annotatedWith(ProduceRateLimitEnabled.class)
    .toInstance(config.getBoolean(KafkaRestConfig.PRODUCE_RATE_LIMIT_ENABLED));
```

Then injected where needed:

```java
public class SomeClass {
  @Inject
  public SomeClass(
      @HostNameConfig String hostName,
      @ProduceRateLimitEnabled Boolean rateLimitEnabled) {
    // ...
  }
}
```

### Lesson for Your Projects

1. Define all configs in one class
2. Use ConfigDef for validation and documentation
3. Bind individual values for injection (not the whole config)
4. Use qualifier annotations for type safety

## Async Response Handling

### The Problem

How do you handle slow operations without blocking threads?

### The Solution

JAX-RS `@Suspended AsyncResponse` with CompletableFuture:

```java
// TopicsResource.java
@GET
public void listTopics(
    @Suspended AsyncResponse asyncResponse,
    @PathParam("clusterId") String clusterId) {

  topicManager.get().listTopics(clusterId)
      .thenApply(topics -> TopicDataList.builder()
          .setData(topics.stream()
              .map(this::toTopicData)
              .collect(toImmutableList()))
          .build())
      .thenApply(Response::ok)
      .whenComplete((builder, error) -> {
        if (error != null) {
          asyncResponse.resume(error);
        } else {
          asyncResponse.resume(builder.build());
        }
      });
}
```

### Pattern: KafkaFutures Conversion

Kafka's `KafkaFuture` is converted to `CompletableFuture`:

```java
// KafkaFutures.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/common/KafkaFutures.java
public static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
  CompletableFuture<T> completableFuture = new CompletableFuture<>();

  kafkaFuture.whenComplete((result, exception) -> {
    if (exception != null) {
      completableFuture.completeExceptionally(exception);
    } else {
      completableFuture.complete(result);
    }
  });

  return completableFuture;
}
```

### Lesson for Your Projects

1. Use `@Suspended AsyncResponse` for I/O-bound operations
2. Convert library futures to CompletableFuture for composition
3. Handle both success and error in `whenComplete`

## Immutable Value Objects

### The Problem

How do you create data transfer objects that are safe, clear, and maintainable?

### The Solution

Google AutoValue for immutable value types:

```java
// TopicData.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/entities/v3/TopicData.java
@AutoValue
@JsonInclude(Include.NON_ABSENT)
public abstract class TopicData {
  public abstract String getClusterId();
  public abstract String getTopicName();
  public abstract boolean isInternal();
  public abstract int getReplicationFactor();
  public abstract int getPartitionsCount();

  public static Builder builder() {
    return new AutoValue_TopicData.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setClusterId(String clusterId);
    public abstract Builder setTopicName(String topicName);
    // ...
    public abstract TopicData build();
  }
}
```

### Benefits

1. **Immutability** - Thread-safe by default
2. **Builder pattern** - Clean construction
3. **Equals/hashCode** - Generated correctly
4. **Less boilerplate** - No getters/setters to write

### Lesson for Your Projects

Use AutoValue (or Lombok/Records) when:
- You have data transfer objects
- Immutability is desirable
- You want correct equals/hashCode

## Extension Point Design

### The Problem

How do you allow third-party extensions without modifying core code?

### The Solution

Define an extension interface and discover implementations via configuration:

```java
// RestResourceExtension.java
// https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/extension/RestResourceExtension.java
public interface RestResourceExtension extends Closeable {
  void register(Configurable<?> config, KafkaRestConfig appConfig);
  default void clean() {}
}
```

Discovery via configuration:

```java
// KafkaRestApplication.java
restResourceExtensions = config.getConfiguredInstances(
    "rest.extension.classes",
    RestResourceExtension.class);
```

### Lesson for Your Projects

1. Define minimal extension interfaces
2. Use configuration for discovery
3. Provide lifecycle hooks (register, clean)

## Key Takeaways

| Pattern | When to Use | Benefit |
|---------|-------------|---------|
| Provider | Request-scoped in singletons | Lazy, testable |
| Factory | Complex creation | Encapsulation |
| Strategy | Multiple algorithms | Extensibility |
| Null Object | Optional features | No null checks |
| Feature | Related JAX-RS components | Organization |
| Delegating Handler | Multi-version APIs | Routing |
| Async Response | I/O operations | Non-blocking |
| AutoValue | Data transfer objects | Safety |
| Extension Interface | Third-party plugins | Flexibility |

## What's Next

In Part 4, we'll explore the rate limiting and resilience patterns that protect REST Proxy and Kafka from abuse.

---

**Code References:**
- [Errors.java](https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/Errors.java)
- [KafkaRestConfig.java](https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/KafkaRestConfig.java)
- [RestResourceExtension.java](https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/extension/RestResourceExtension.java)
