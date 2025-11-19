# Kafka REST Proxy - Quick Start Analysis

**Commit SHA:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## What is Kafka REST Proxy?

Kafka REST Proxy provides a RESTful interface to Apache Kafka clusters, enabling:
- **Produce messages** without native Kafka clients
- **Consume messages** via HTTP polling
- **Administer clusters** through REST APIs
- **Integrate with Schema Registry** for schema validation

## High-Level Architecture

```
┌─────────────┐     ┌─────────────────────────────────────┐     ┌─────────────┐
│   Client    │────▶│         Kafka REST Proxy            │────▶│    Kafka    │
│  (HTTP)     │     │  ┌─────────┐ ┌──────────┐ ┌──────┐  │     │   Cluster   │
└─────────────┘     │  │Resources│→│Controllers│→│Backends│ │     └─────────────┘
                    │  └─────────┘ └──────────┘ └──────┘  │
                    └─────────────────────────────────────┘
                                      ↓
                              ┌─────────────────┐
                              │ Schema Registry │
                              └─────────────────┘
```

## Key Entry Points

| Entry Point | Location | Purpose |
|-------------|----------|---------|
| Main Class | `KafkaRestMain.java:29` | CLI entry point |
| Application | `KafkaRestApplication.java:62` | DI configuration |
| Config | `KafkaRestConfig.java:67` | Configuration definitions |

GitHub URL format for code references:
```
https://github.com/confluentinc/kafka-rest/blob/28ae7f33556978fa8ee59bab75b27301d1222622/kafka-rest/src/main/java/io/confluent/kafkarest/{path}
```

## Core Abstractions

### 1. Resources (REST Endpoints)
- **V2:** `/v2/topics`, `/v2/consumers` - Legacy API
- **V3:** `/v3/clusters/{clusterId}/...` - Modern cluster-centric API

### 2. Controllers (Business Logic)
- `ProduceController` - Message production
- `TopicManager` - Topic CRUD operations
- `ConsumerManager` - Consumer group management
- `SchemaManager` - Schema Registry integration

### 3. Backends (External Systems)
- `KafkaModule` - Kafka Admin/Producer/Consumer clients
- `SchemaRegistryModule` - Schema Registry client

## Quick Configuration

Minimal `kafka-rest.properties`:
```properties
bootstrap.servers=localhost:9092
listeners=http://0.0.0.0:8082
```

With Schema Registry:
```properties
bootstrap.servers=localhost:9092
listeners=http://0.0.0.0:8082
schema.registry.url=http://localhost:8081
```

## API Examples

### V3 API (Recommended)

**Get cluster info:**
```bash
curl http://localhost:8082/v3/clusters
```

**Produce a message:**
```bash
curl -X POST http://localhost:8082/v3/clusters/{clusterId}/topics/test/records \
  -H "Content-Type: application/json" \
  -d '{"value":{"type":"JSON","data":{"key":"value"}}}'
```

### V2 API (Legacy)

**List topics:**
```bash
curl http://localhost:8082/topics
```

**Produce a message:**
```bash
curl -X POST http://localhost:8082/topics/test \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records":[{"value":{"name":"test"}}]}'
```

## Important Classes to Understand

| Class | Lines | Purpose |
|-------|-------|---------|
| `KafkaRestConfig` | 1,488 | All configuration options |
| `ProduceAction` | ~300 | V3 produce endpoint logic |
| `ProduceControllerImpl` | ~150 | Core produce business logic |
| `KafkaConsumerManager` | ~600 | V2 consumer lifecycle management |
| `DefaultKafkaRestContext` | ~200 | Kafka client creation |

## Design Patterns Used

1. **Dependency Injection** - HK2 for loose coupling
2. **Provider Pattern** - Lazy initialization of dependencies
3. **Strategy Pattern** - Pluggable rate limiters, serializers
4. **Factory Pattern** - Client and rate limiter creation
5. **Facade Pattern** - Resources hide controller complexity

## Performance Characteristics

- **Produce:** Single shared KafkaProducer, async with CompletableFuture
- **Consume:** Per-consumer thread, bounded thread pool (default 50)
- **Admin:** Async AdminClient operations

## Next Steps

1. Read `repository-structure.md` for directory layout
2. Read `dependency-graph.md` for dependency relationships
3. Read blog series for deep dives into specific components
4. Review RFCs for improvement opportunities
