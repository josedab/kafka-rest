# KAFKA-REST - KEY ENTRY POINTS AND FILE REFERENCES

## 1. PRODUCE FLOW - DETAILED PATH

### V3 API (Modern, Recommended)
```
HTTP POST /v3/clusters/{clusterId}/topics/{topicName}/records
    ↓
ProduceAction.produce()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java:141]
    
    ├─→ Rate Limiting (Line 175-181)
    │   └─→ ProduceRateLimiters.rateLimit()
    │
    ├─→ Schema Resolution (Line 196-212)
    │   ├─→ getSchema() for key [Line 247-271]
    │   ├─→ getSchema() for value
    │   └─→ SchemaManager.getSchema()
    │
    ├─→ Serialization (Line 273-287)
    │   └─→ SchemaRecordSerializerImpl.serialize()
    │       [File: /kafka-rest/src/main/java/io/confluent/kafkarest/controllers/SchemaRecordSerializerImpl.java:69]
    │       ├─→ serializeAvro() [Line 102]
    │       ├─→ serializeJsonschema() [Line 113]
    │       └─→ serializeProtobuf() [Line 124]
    │
    ├─→ Kafka Producer Send (Line 214-222)
    │   └─→ ProduceControllerImpl.produce()
    │       [File: /kafka-rest/src/main/java/io/confluent/kafkarest/controllers/ProduceControllerImpl.java:46]
    │       └─→ producer.send(ProducerRecord, Callback)
    │           ├─→ Creates ProducerRecord [Line 57-69]
    │           └─→ Callback handler [Line 70-77]
    │
    └─→ Response Composition (Line 224-244)
        └─→ toProduceResponse() [Line 289-325]
            └─→ ProduceResponse with partition, offset, timestamp, schemas
```

### V2 API (Legacy)
```
HTTP POST /topics/{topicName}
    ↓
AbstractProduceAction
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/AbstractProduceAction.java]
    
    ├─→ produceWithSchema() [Line 87]
    │   └─→ getSchema() [Line 111]
    │       └─→ SchemaManager.getSchema()
    │
    ├─→ serialize() [Line 135]
    │   └─→ RecordSerializer.serialize()
    │
    ├─→ doProduce() [Line 166]
    │   └─→ ProduceController.produce() × N records
    │
    └─→ produceResultsToResponse() [Line 184]
        └─→ Batch results with error handling [Line 219-237]
```

---

## 2. CONSUME FLOW - DETAILED PATH

### V2 API (Primary)
```
HTTP POST /consumers/{group}/instances
    ↓
ConsumersResource.createGroup()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java:97]
    └─→ KafkaConsumerManager.createConsumer()
        [File: /kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerManager.java:169]
        ├─→ getConsumerInstanceProperties() [Line 237]
        ├─→ new KafkaConsumer(props) [Line 196-200]
        └─→ createConsumerState() [Line 206]

HTTP POST /consumers/{group}/instances/{instance}/subscription
    ↓
ConsumersResource.subscribe()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java:129]
    └─→ KafkaConsumerState.subscribe()
        [File: /kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java:222]
        ├─→ consumer.subscribe(topics) [Line 229-230]
        └─→ consumer.subscribe(pattern) [Line 231-234]

HTTP GET /consumers/{group}/instances/{instance}/records
    ↓
ConsumersResource.readRecordBinary() (multiple variants for formats)
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java:167]
    ├─→ readRecords() [Line 391]
    │   └─→ KafkaConsumerManager.readRecords() [Line 323]
    │       ├─→ getConsumerInstance(group, instance) [Line 332]
    │       └─→ new KafkaConsumerReadTask() [Line 344-346]
    │           └─→ executor.submit(RunnableReadTask) [Line 347]
    │
    └─→ Poll Loop
        ├─→ RunnableReadTask.run()
        │   [File: /kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerManager.java:429]
        │   └─→ KafkaConsumerReadTask.doPartialRead() [Line 431]
        │       [File: /kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerReadTask.java:92]
        │       ├─→ addRecords() [Line 100]
        │       │   └─→ parent.hasNext() [Line 140]
        │       │       └─→ getOrCreateConsumerRecords() [Line 367]
        │       │           └─→ consumer.poll(0) [Line 386]
        │       └─→ Check completion [Line 114-124]
        │
        └─→ Callback onCompletion()
            └─→ AsyncResponse.resume(records)
```

### Consumer Offset Management
```
HTTP POST /consumers/{group}/instances/{instance}/offsets
    ↓
ConsumersResource.commitOffsets()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java:272]
    └─→ KafkaConsumerManager.commitOffsets()
        [File: /kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerManager.java:475]
        ├─→ getConsumerInstance() [Line 481]
        └─→ executor.submit() → state.commitOffsets() [Line 493-495]
            ├─→ consumer.commitSync() [Line 114]
            └─→ consumer.commitSync(offsetMap) [Line 134]
```

### V3 API (Metadata-focused, limited consume)
```
HTTP GET /v3/clusters/{clusterId}/consumer-groups/{consumerGroupId}/consumers
    ↓
ConsumersResource.listConsumers()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ConsumersResource.java:64]
    └─→ ConsumerManager.listConsumers() [Line 74]
        └─→ Returns consumer metadata (no record consumption)
```

---

## 3. ADMIN OPERATIONS FLOW

### Topic Management
```
HTTP GET /v3/clusters/{clusterId}/topics
    ↓
TopicsResource.listTopics()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicsResource.java:85]
    └─→ TopicManager.listTopics()
        [File: /kafka-rest/src/main/java/io/confluent/kafkarest/controllers/TopicManagerImpl.java:64]
        ├─→ adminClient.listTopics().listings() [Line 70]
        └─→ describeTopics() [Line 76-79]

HTTP POST /v3/clusters/{clusterId}/topics
    ↓
TopicsResource.createTopic()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicsResource.java:173]
    ├─→ Validate topic name [Line 182-187]
    └─→ TopicManager.createTopic2()
        [File: /kafka-rest/src/main/java/io/confluent/kafkarest/controllers/TopicManagerImpl.java]
        └─→ adminClient.createTopics()

HTTP PATCH /v3/clusters/{clusterId}/topics/{topicName}
    ↓
TopicsResource.updatePartitionsCount()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicsResource.java:138]
    └─→ TopicManager.updateTopicPartitionsCount()
```

### Broker Information
```
HTTP GET /v3/clusters/{clusterId}/brokers
    ↓
BrokersResource.listBrokers()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/BrokersResource.java:62]
    └─→ BrokerManager.listBrokers()
        └─→ adminClient.describeCluster()
```

### Configuration Management
```
HTTP GET /v3/clusters/{clusterId}/topics/{topicName}/configs
    ↓
TopicConfigsResource.listTopicConfigs()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicConfigsResource.java]
    └─→ TopicConfigManager
        └─→ adminClient.describeConfigs()
```

---

## 4. ERROR HANDLING FLOW

### Exception Creation
```
Exception Thrown
    ↓
Errors.* methods
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/Errors.java]
    
    Examples:
    ├─→ topicNotFoundException() [Line 41]
    ├─→ keySchemaMissingException() [Line 120]
    ├─→ messageSerializationException() [Line 181]
    ├─→ produceBatchException() [Line 204]
    └─→ kafkaErrorException() [Line 214]
```

### Exception Mapping to HTTP Response

#### V3 API
```
StatusCodeException
    ↓
V3ExceptionMapper.toResponse()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/v3/V3ExceptionMapper.java:24]
    └─→ ErrorResponse.create()
        └─→ HTTP Response with error code + message
```

#### V2 API
```
StatusCodeException
    ↓
V2ExceptionMapper.toResponse()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/v2/V2ExceptionMapper.java:24]
    └─→ ErrorResponse.create()
```

#### Kafka-Specific Exceptions
```
Kafka Exception
    ↓
KafkaRestExceptionMapper.toResponse()
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/KafkaRestExceptionMapper.java:31]
    ├─→ SerializationException → 408 Request Timeout (40801)
    └─→ Other KafkaException → delegate to KafkaExceptionMapper

Producer Exception Mapping
    [File: /kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/AbstractProduceAction.java:219]
    ├─→ AuthenticationException → 40101
    ├─→ AuthorizationException → 40102
    ├─→ RetriableException → 40003
    └─→ KafkaException → 40000
```

### Error Code Constants
**File**: `/kafka-rest/src/main/java/io/confluent/kafkarest/Errors.java`
- Authentication: 40101
- Authorization: 40102
- Retriable: 40003
- Topic Not Found: 40401
- Partition Not Found: 40402
- Consumer Instance Not Found: 40403
- Key Schema Missing: 42201
- Value Schema Missing: 42202
- Serialization Error: 42207
- Invalid Payload: 42206

---

## 5. KEY ARCHITECTURAL COMPONENTS

### Request Processing
- **Async Model**: JAX-RS `AsyncResponse` with `@Suspended`
- **Streaming**: `JsonStream<T>` for request streaming (v3 produce)
- **Callbacks**: Event-based completion for async operations

### Data Serialization
**File**: `/kafka-rest/src/main/java/io/confluent/kafkarest/controllers/`
- `RecordSerializer` - Interface for serialization
- `SchemaRecordSerializerImpl` - Implements AVRO, JSON Schema, Protobuf
- `NoSchemaRecordSerializer` - Binary format handler

### Thread Pool Management
**Consume (v2)**:
- `KafkaConsumerThreadPoolExecutor` [Line 360 in KafkaConsumerManager]
- `ReadTaskSchedulerThread` for task re-scheduling
- `DelayQueue` for backoff management

**Produce (v3)**:
- `@ProduceResponseThreadPool ExecutorService`
- Async composition with `CompletableFuture`

### Consumer State Management
**File**: `/kafka-rest/src/main/java/io/confluent/kafkarest/v2/`
- `KafkaConsumerState` - Base consumer state
- `BinaryKafkaConsumerState` - Binary format
- `JsonKafkaConsumerState` - JSON format
- `SchemaKafkaConsumerState` - AVRO/JSON Schema/Protobuf

### Futures Bridging
**File**: `/kafka-rest/src/main/java/io/confluent/kafkarest/common/`
- `KafkaFutures.toCompletableFuture()` - Convert Kafka futures to Java futures
- `CompletableFutures.allAsList()` - Aggregate futures

---

## 6. PERFORMANCE OPTIMIZATIONS

### Stack Trace Avoidance
**File**: `/kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/StacklessCompletionException.java`
- Custom exception that overrides `fillInStackTrace()` 
- Used for non-critical errors (rate limiting) to avoid GC pressure

### Record Buffering
**File**: `/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- `ArrayDeque<ConsumerRecord>` for buffering [Line 73]
- Cached checks before polling [Line 372-374]

### Metrics Recording
**File**: `/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- Request metrics [Line 185]
- Response metrics [Line 327-330]
- Error metrics [Line 332-335]
- Rate limit metrics [Line 337-339]
