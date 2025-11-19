# Kafka REST Proxy - Dependency Graph

**Commit SHA:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Dependency Injection Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    KafkaRestApplication                         │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ BackendsModule│  │ConfigModule │  │ControllersModule│         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│         │                │                │                      │
│         ▼                ▼                ▼                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ KafkaModule │  │ Properties  │  │  Managers   │              │
│  │ SR Module   │  │  Bindings   │  │  Bindings   │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ExceptionsModule│ │RateLimitFeature│ │ResourcesFeature│        │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

## Runtime Dependencies (Maven)

### Core Dependencies

| Dependency | Version | Purpose | Last Update |
|------------|---------|---------|-------------|
| `rest-utils` | 8.2.x | Base REST framework | Current |
| `kafka-clients` | ${kafka.version} | Kafka Java client | Current |
| `jackson-databind` | ${jackson.version} | JSON serialization | Current |
| `guava` | ${guava.version} | Collections, caching | Current |
| `resilience4j-ratelimiter` | 1.7.1 | Rate limiting | 2021 |
| `auto-value-annotations` | 1.7.2 | Immutable value types | 2020 |
| `jersey-server` | 3.x | JAX-RS implementation | Current |
| `jetty-server` | ${jetty.version} | HTTP server | Current |

### Schema Registry Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| `kafka-avro-serializer` | ${sr.version} | Avro serialization |
| `kafka-json-serializer` | ${sr.version} | JSON serialization |
| `kafka-json-schema-serializer` | ${sr.version} | JSON Schema validation |
| `kafka-protobuf-serializer` | ${sr.version} | Protobuf serialization |

### Test Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| `junit-jupiter-*` | ${junit.jupiter.version} | Unit testing |
| `easymock` | ${easymock.version} | Mocking |
| `hamcrest-all` | ${hamcrest.version} | Assertions |
| `kafka_${scala.version}:test` | ${kafka.version} | Kafka test utilities |
| `equalsverifier-nodep` | 3.18.2 | Equals/hashCode testing |

## Internal Module Dependencies

### Layer Dependencies

```
┌─────────────────────────────────────────────────────────────┐
│                      Resources Layer                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐         │
│  │ Topics  │  │ Brokers │  │ Produce │  │ Consume │         │
│  │Resource │  │Resource │  │ Action  │  │Resource │         │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘         │
│       │            │            │            │               │
└───────┼────────────┼────────────┼────────────┼───────────────┘
        │            │            │            │
        ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Controllers Layer                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐         │
│  │ Topic   │  │ Broker  │  │ Produce │  │ Consumer│         │
│  │ Manager │  │ Manager │  │Controller│ │ Manager │         │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘         │
│       │            │            │            │               │
└───────┼────────────┼────────────┼────────────┼───────────────┘
        │            │            │            │
        ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│                     Backends Layer                           │
│  ┌───────────────────────────────────────────────────┐      │
│  │              KafkaRestContext                      │      │
│  │  ┌──────┐  ┌──────────┐  ┌──────────┐             │      │
│  │  │Admin │  │ Producer │  │ Consumer │             │      │
│  │  │Client│  │<byte[],  │  │<byte[],  │             │      │
│  │  └──────┘  │ byte[]>  │  │ byte[]>  │             │      │
│  │            └──────────┘  └──────────┘             │      │
│  └───────────────────────────────────────────────────┘      │
│                                                             │
│  ┌───────────────────────────────────────────────────┐      │
│  │           SchemaRegistryClient (Optional)          │      │
│  └───────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### Cross-Cutting Concerns

```
┌─────────────────────────────────────────────┐
│              All Layers Use:                │
│                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │ Config  │  │ Entities│  │ Logging │     │
│  │ Module  │  │  (DTOs) │  │ (SLF4J) │     │
│  └─────────┘  └─────────┘  └─────────┘     │
│                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │  Rate   │  │Exception│  │ Metrics │     │
│  │ Limiter │  │ Mappers │  │         │     │
│  └─────────┘  └─────────┘  └─────────┘     │
└─────────────────────────────────────────────┘
```

## Dependency Analysis

### Potential Issues

1. **resilience4j 1.7.1** - Released 2021, current is 2.x
   - Consider upgrade for bug fixes and features

2. **auto-value 1.7.2** - Released 2020, current is 1.10+
   - Stable, but may benefit from newer features

3. **Google Java Format 1.7** - Enforced via Spotless
   - Current version is 1.18+, consider upgrade

### License Compatibility

| Dependency | License | Compatible |
|------------|---------|------------|
| Kafka Clients | Apache 2.0 | Yes |
| Jackson | Apache 2.0 | Yes |
| Guava | Apache 2.0 | Yes |
| Resilience4J | Apache 2.0 | Yes |
| Jersey | EPL 2.0 | Yes |
| Jetty | Apache 2.0/EPL | Yes |

### Heavyweight Dependencies

| Dependency | Size Impact | Lighter Alternative |
|------------|-------------|---------------------|
| Guava | ~3MB | Caffeine (for caching only) |
| Jackson Datatype Guava | ~100KB | Custom serializers |

## HK2 Binding Summary

### Singleton Bindings
- `Admin` - Kafka admin client
- `Producer<byte[], byte[]>` - Kafka producer
- `KafkaRestContext` - Context facade
- `SchemaRecordSerializer` - Serializer

### Request-Scoped Bindings
- `Optional<SchemaRegistryClient>` - Schema Registry client
- `UrlFactory` - URL generation
- `ExecutorService` - Produce response pool

### Factory Bindings
- `RequestRateLimiterFactory` - Rate limiter creation
- `SchemaRecordSerializerFactory` - Serializer creation
