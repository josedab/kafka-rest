# Kafka REST Proxy - Terminology Glossary

**Commit SHA:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Core Concepts

### REST Proxy
The HTTP server that translates REST API calls into Kafka protocol operations, enabling non-native Kafka clients to interact with Kafka clusters.

### V2 API
Legacy REST API with simpler paths (`/topics`, `/consumers`). Maintained for backward compatibility. Includes consumer group management for HTTP-based consumption.

### V3 API
Modern REST API with cluster-scoped paths (`/v3/clusters/{clusterId}/...`). Provides richer metadata and comprehensive cluster management operations.

### KafkaRestContext
Central facade interface (`KafkaRestContext.java`) that provides access to Kafka clients (Admin, Producer, Consumer) and configuration. Implemented by `DefaultKafkaRestContext`.

## Architecture Terms

### Resource
JAX-RS REST endpoint class annotated with `@Path`. Handles HTTP requests and delegates to controllers. Examples: `TopicsResource`, `BrokersResource`, `ProduceAction`.

### Controller/Manager
Business logic layer components that implement operations on Kafka entities. Interfaces define contracts, implementations handle actual Kafka operations. Examples: `TopicManager`, `ProduceController`, `BrokerConfigManager`.

### Backend
Layer responsible for external system integration. Contains modules for creating and managing Kafka clients and Schema Registry clients.

### Module
HK2 dependency injection configuration class extending `AbstractBinder`. Binds interfaces to implementations. Examples: `BackendsModule`, `ControllersModule`, `ConfigModule`.

### Feature
JAX-RS feature that registers related components with Jersey. Examples: `RateLimitFeature`, `ResourcesFeature`, `ResourceAccesslistFeature`.

## API Terms

### Embedded Format
Serialization format for message data. Options: `BINARY`, `JSON`, `AVRO`, `PROTOBUF`, `JSONSCHEMA`. Defined in `EmbeddedFormat.java`.

### ProduceRequest
V3 API request body for producing messages. Contains data type, optional key, value, headers, partition ID, and timestamp.

### ProduceResult
Internal representation of a successful produce operation. Contains offset, partition, timestamp, and serialized sizes.

### AsyncResponse
JAX-RS mechanism for asynchronous endpoint handling. Used in V3 endpoints with `@Suspended AsyncResponse` parameter to release request thread while waiting for Kafka operations.

### CRN (Confluent Resource Name)
Unique identifier format for Kafka resources: `crn:///kafka={clusterId}/topic={topicName}`. Used for resource identification in responses.

## Consumer Terms

### Consumer Instance
V2 API concept of a long-lived consumer with unique ID (`ConsumerInstanceId`). Created, subscribed, polled, and destroyed via REST API.

### Consumer Group
Kafka consumer group used for coordinated consumption. V2 API manages consumer groups through REST endpoints.

### KafkaConsumerManager
V2 component managing consumer instance lifecycle, threading, and timeouts. Located at `v2/KafkaConsumerManager.java`.

### Consumer State
Internal state object (`KafkaConsumerState`) holding consumer configuration, subscriptions, and buffered records.

## Rate Limiting Terms

### RequestRateLimiter
Interface for rate limiting implementations. Implementations: `GuavaRateLimiter`, `Resilience4JRateLimiter`, `NullRequestRateLimiter`.

### Rate Limit Backend
Pluggable rate limiting algorithm. Options: `guava` (default), `resilience4j`. Configured via `rate.limit.backend`.

### Fixed-Cost Rate Limiting
Per-endpoint rate limiting with configurable cost per request. Different endpoints can have different costs.

### @DoNotRateLimit
Annotation to exclude specific operations from rate limiting. Used on schema resolution operations.

## Schema Registry Terms

### Schema Manager
Controller for Schema Registry operations. Resolves schemas, registers new schemas, and retrieves schema metadata.

### Subject Name Strategy
Strategy for determining Schema Registry subject names. Examples: `TopicNameStrategy`, `RecordNameStrategy`, `TopicRecordNameStrategy`.

### Schema Record Serializer
Component that serializes produce requests using Schema Registry. Handles Avro, Protobuf, and JSON Schema formats.

## Configuration Terms

### KafkaRestConfig
Main configuration class extending `RestConfig`. Defines all configuration options with defaults, documentation, and validation.

### Listeners
Network interfaces where REST Proxy listens for connections. Format: `http://host:port` or `https://host:port`.

### Bootstrap Servers
Initial Kafka broker addresses for client connection. Required configuration: `bootstrap.servers`.

## Exception Terms

### StatusCodeException
Base exception class for REST errors with HTTP status code and error code.

### Error Code
Numeric code for specific error conditions. Format: 4XXXX for client errors, 5XXXX for server errors. Defined in `Errors.java`.

### ExceptionMapper
JAX-RS component that converts exceptions to HTTP responses. Version-specific: `V2ExceptionMapper`, `V3ExceptionMapper`.

## Extension Terms

### RestResourceExtension
Plugin interface for extending REST Proxy functionality. Enterprise security plugins implement this interface.

### Resource Access Control
Configuration-based endpoint filtering via `api.endpoints.allowlist` and `api.endpoints.blocklist`.

### @ResourceName
Annotation on resources for fine-grained access control. Example: `@ResourceName("api.v3.topics.*")`.

## Build/Deploy Terms

### Assembly
Maven assembly for packaging. Types: `development` (for local dev), `package` (distribution), `standalone` (self-contained JAR).

### bin Scripts
Shell scripts for starting/stopping REST Proxy:
- `kafka-rest-start` - Start the service
- `kafka-rest-stop` - Stop the service
- `kafka-rest-run-class` - Run arbitrary class

## Testing Terms

### ClusterTestHarness
Base class for integration tests that starts embedded Kafka cluster.

### KafkaClusterFixture
JUnit 5 extension that manages Kafka cluster lifecycle for tests.

### Test Environment
Configurable test setup with different security configurations (minimal, mTLS, SASL).

## Metrics Terms

### @PerformanceMetric
Annotation on resources for automatic metric collection. Example: `@PerformanceMetric("v3.topics.list")`.

### KafkaRestMetricsContext
Wrapper around Kafka Metrics for REST Proxy. Adds resource type, version, and cluster ID labels.

### Telemetry
Integration with Confluent's telemetry system via `metrics.reporters` configuration.
