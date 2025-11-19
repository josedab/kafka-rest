# Kafka REST Proxy - Repository Structure

**Commit SHA:** `28ae7f33556978fa8ee59bab75b27301d1222622`

## Top-Level Structure

```
kafka-rest/
├── api/                    # OpenAPI specifications
├── bin/                    # Startup scripts
├── checkstyle/             # Code style configuration
├── config/                 # Sample configuration files
├── debian/                 # Debian packaging
├── examples/               # Example configurations
├── kafka-rest/             # Main source module
├── licenses/               # Third-party licenses
├── testing/                # Testing environments
├── pom.xml                 # Parent Maven POM
└── README.md               # Project documentation
```

## Main Source Module (`kafka-rest/src/main/java/io/confluent/kafkarest/`)

### Core Package Structure

```
kafkarest/
├── KafkaRestMain.java          # CLI entry point
├── KafkaRestApplication.java   # Application bootstrap
├── KafkaRestConfig.java        # Configuration definitions (1,488 LOC)
├── KafkaRestContext.java       # Context interface
├── DefaultKafkaRestContext.java # Context implementation
├── Errors.java                 # Error code definitions
├── Versions.java               # API version constants
│
├── backends/                   # External system integration
│   ├── BackendsModule.java     # Root DI module
│   ├── kafka/                  # Kafka client management
│   │   └── KafkaModule.java
│   └── schemaregistry/         # Schema Registry client
│       └── SchemaRegistryModule.java
│
├── controllers/                # Business logic layer (18 managers)
│   ├── ControllersModule.java  # DI bindings
│   ├── ProduceController.java  # Produce interface
│   ├── TopicManager.java       # Topic operations
│   ├── BrokerManager.java      # Broker metadata
│   ├── ClusterManager.java     # Cluster metadata
│   ├── ConsumerManager.java    # Consumer management
│   ├── SchemaManager.java      # Schema operations
│   └── ...
│
├── resources/                  # REST endpoints
│   ├── ResourcesFeature.java   # Version routing
│   ├── v2/                     # V2 API (9 resources)
│   │   ├── V2ResourcesFeature.java
│   │   ├── TopicsResource.java
│   │   ├── ConsumersResource.java
│   │   └── ...
│   └── v3/                     # V3 API (31 resources)
│       ├── V3ResourcesFeature.java
│       ├── V3ResourcesModule.java
│       ├── ClustersResource.java
│       ├── TopicsResource.java
│       ├── ProduceAction.java
│       ├── ProduceBatchAction.java
│       └── ...
│
├── entities/                   # Data transfer objects (144 classes)
│   ├── EmbeddedFormat.java
│   ├── ProduceResult.java
│   ├── v2/                     # V2-specific models
│   └── v3/                     # V3-specific models
│       ├── ProduceRequest.java
│       ├── ProduceResponse.java
│       ├── TopicData.java
│       └── ...
│
├── config/                     # Configuration module
│   ├── ConfigModule.java
│   └── SchemaRegistryConfig.java
│
├── exceptions/                 # Error handling
│   ├── ExceptionsModule.java
│   ├── StatusCodeException.java
│   ├── v2/
│   │   └── V2ExceptionMapper.java
│   └── v3/
│       └── V3ExceptionMapper.java
│
├── extension/                  # Extension points
│   ├── RestResourceExtension.java  # Plugin interface
│   ├── ResourceAccesslistFeature.java
│   ├── EnumConverterProvider.java
│   └── InstantConverterProvider.java
│
├── ratelimit/                  # Rate limiting
│   ├── RateLimitFeature.java
│   ├── RequestRateLimiter.java
│   ├── GuavaRateLimiter.java
│   ├── Resilience4JRateLimiter.java
│   └── ...
│
├── requestlog/                 # Request logging
│   ├── CustomLog.java
│   └── CustomLogRequestAttributes.java
│
├── response/                   # Response formatting
│   ├── ResponseModule.java
│   └── JsonStreamMessageBodyReader.java
│
├── converters/                 # Data converters
├── common/                     # Shared utilities
├── tools/                      # Command-line tools
└── v2/                         # V2-specific components
    └── KafkaConsumerManager.java
```

## Test Structure (`kafka-rest/src/test/`)

```
test/
├── java/io/confluent/kafkarest/
│   ├── TestUtils.java              # Common test utilities
│   ├── controllers/                # Controller unit tests
│   ├── resources/
│   │   ├── v2/                     # V2 resource tests
│   │   └── v3/                     # V3 resource tests
│   ├── integration/                # Integration tests
│   │   ├── ClusterTestHarness.java # Base harness
│   │   ├── v2/                     # V2 integration tests
│   │   └── v3/                     # V3 integration tests
│   ├── testing/                    # Test fixtures
│   │   ├── KafkaClusterFixture.java
│   │   ├── SchemaRegistryFixture.java
│   │   └── DefaultKafkaRestTestEnvironment.java
│   └── mock/                       # Mock implementations
│
└── resources/
    └── log4j2-test.yaml            # Test logging config
```

## Configuration Files

| File | Purpose |
|------|---------|
| `config/kafka-rest.properties` | Sample configuration |
| `config/log4j2.yaml` | Logging configuration |
| `checkstyle/checkstyle.xml` | Code style rules |
| `checkstyle/suppressions.xml` | Style rule exceptions |

## Build Artifacts

| Artifact | Description |
|----------|-------------|
| `kafka-rest-{version}.jar` | Core library |
| `kafka-rest-{version}-package.tar.gz` | Distribution package |
| `kafka-rest-{version}-standalone.jar` | Self-contained executable |

## Key Files by Size (Production Code)

| File | Lines | Purpose |
|------|-------|---------|
| `KafkaRestConfig.java` | 1,488 | Configuration definitions |
| `KafkaConsumerManager.java` | ~600 | Consumer lifecycle |
| `Errors.java` | ~300 | Error codes |
| `ProduceAction.java` | ~300 | V3 produce endpoint |
| `KafkaRestApplication.java` | 243 | Application bootstrap |

## Package Dependencies

```
resources/ ──depends on──▶ controllers/ ──depends on──▶ backends/
    │                           │                           │
    ▼                           ▼                           ▼
entities/                  entities/                   kafka clients
                          config/
```
