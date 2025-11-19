# Kafka-REST Data Flow Analysis

## 1. PRODUCE FLOW

### Entry Point: HTTP Request to Resource
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 70**: REST endpoint definition: `@Path("/v3/clusters/{clusterId}/topics/{topicName}/records")`
- **Line 141-165**: `produce()` method - Main entry point for HTTP POST requests
  - Receives `AsyncResponse` for asynchronous handling
  - Receives `JsonStream<ProduceRequest>` containing produce records
  - Uses `streamingResponseFactory.from()` to compose request stream processing

### 1.1 Request Validation & Rate Limiting
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 175-181**: Rate limit check
  - Calls `produceRateLimiters.rateLimit(clusterId, request.getOriginalSize(), httpServletRequest)`
  - Throws `RateLimitExceededException` wrapped in `StacklessCompletionException` (avoids costly stack traces)

### 1.2 Schema Resolution
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 196-212**: Schema handling for key and value
  - **Line 197**: `getSchema()` for key schema
  - **Line 206**: `getSchema()` for value schema
  - **Line 247-271**: `getSchema()` method
    - Delegates to `SchemaManager.getSchema()`
    - Returns `RegisteredSchema` containing schema ID, version, format
    - Can throw `SerializationException` or `IllegalArgumentException`

### 1.3 Serialization
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 202-212**: Serialization of key and value
  - Calls `serialize()` method at line 273-287
  - **Line 273-287**: `serialize()` method
    - Uses `RecordSerializerProvider.get().serialize()`
    - Handles format: BINARY, AVRO, JSONSCHEMA, PROTOBUF
    - Returns `Optional<ByteString>` (empty if null)

**Schema Serialization Details**:
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/controllers/SchemaRecordSerializerImpl.java`
- **Line 69-100**: Main `serialize()` method
- **Line 86-99**: Format-specific serialization
  - **Line 87-88**: AVRO - calls `serializeAvro()` (line 102-111)
  - **Line 90-91**: JSONSCHEMA - calls `serializeJsonschema()` (line 113-122)
  - **Line 93-95**: PROTOBUF - calls `serializeProtobuf()` (line 124-135)

### 1.4 Kafka Producer Interaction
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 214-222**: Call to ProduceController
  - `controller.produce()` returns `CompletableFuture<ProduceResult>`
  - Passes:
    - clusterId, topicName, partitionId (optional)
    - headers, serialized key/value, timestamp

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/controllers/ProduceControllerImpl.java`
- **Line 46-80**: `produce()` implementation
  - **Line 56-69**: Constructs `ProducerRecord`:
    - Line 57: Topic name
    - Line 58: Partition ID (null if not specified for default partitioning)
    - Line 60: Timestamp (from request or current time)
    - Line 61: Key bytes
    - Line 62: Value bytes
    - Line 63-68**: Headers as `RecordHeader` objects
  - **Line 56**: Calls `producer.send()` with callback
  - **Line 70-77**: Callback handler:
    - On success: Extracts metadata and completes future
    - On exception: Completes future exceptionally

### 1.5 Response Formation
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 224-244**: Async response handling
  - **Line 225-234**: Error handling in `handleAsync()`
  - **Line 235-244**: Success path in `thenApplyAsync()`
    - **Line 237-239**: `toProduceResponse()` constructs response
- **Line 289-325**: `toProduceResponse()` method
  - Creates `ProduceResponse` with:
    - Partition ID from result (line 300)
    - Offset from result (line 301)
    - Timestamp (line 302)
    - Key format, schema info, size (line 303-312)
    - Value format, schema info, size (line 313-322)
    - HTTP status 200 (line 323)

### 1.6 Metrics Recording
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 327-345**: Metrics recording methods
  - `recordResponseMetrics()` (line 327-330)
  - `recordErrorMetrics()` (line 332-335)
  - `recordRateLimitedMetrics()` (line 337-339)
  - `recordRequestMetrics()` (line 341-345)

---

## 2. CONSUME FLOW

### 2.1 Consumer Instance Creation
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java`
- **Line 97-118**: `createGroup()` method
  - **Line 109-113**: Calls `KafkaConsumerManager.createConsumer()`
  - Returns consumer instance ID

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerManager.java`
- **Line 169-219**: `createConsumer()` method
  - **Line 176-177**: Generates unique consumer name
  - **Line 188-209**: Creates KafkaConsumer
    - **Line 193**: Gets consumer properties from config
    - **Line 196-200**: Instantiates `KafkaConsumer` with properties
    - **Line 206-208**: Wraps consumer in appropriate state wrapper (Binary/Schema/JSON)
  - **Line 237-293**: `getConsumerInstanceProperties()` method
    - Configures deserializers based on format (BINARY, AVRO, JSON, JSONSCHEMA, PROTOBUF)

### 2.2 Subscription Management
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java`
- **Line 129-143**: `subscribe()` method
  - Calls `KafkaConsumerManager.subscribe()`

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- **Line 222-237**: `subscribe()` method
  - **Line 229-230**: Subscribe to specific topics list
  - **Line 231-234**: Subscribe to topic pattern with regex

### 2.3 Polling & Record Fetching
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java`
- **Line 167-186**: `readRecordBinary()` - example read endpoint
  - **Line 178-186**: Calls `readRecords()` helper method
- **Line 391-423**: `readRecords()` helper method
  - **Line 402-410**: Calls `KafkaConsumerManager.readRecords()`
  - **Line 411-421**: Callback handling for async completion

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerManager.java`
- **Line 323-348**: `readRecords()` method
  - **Line 332-342**: Gets consumer instance and validates format
  - **Line 344-347**: Creates `KafkaConsumerReadTask` and submits to executor
  - Uses thread pool with backoff mechanism for continuous polling

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerReadTask.java`
- **Line 63-90**: Constructor sets up read parameters
  - **Line 70-84**: Configures timeout, max bytes, min bytes
- **Line 92-129**: `doPartialRead()` - Main polling loop
  - **Line 100**: Calls `addRecords()` to fetch records
  - **Line 114-124**: Checks completion conditions (timeout/max bytes)
  - **Line 139-150**: `addRecords()` method
    - **Line 140-145**: Loops while minimum bytes not exceeded
    - **Line 142-144**: Synchronized access to consumer
    - Calls `maybeAddRecord()` for each record

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- **Line 362-370**: `hasNext()` method
  - **Line 363-364**: Checks cached records
  - **Line 367**: Calls `getOrCreateConsumerRecords()` if needed
- **Line 385-391**: `getOrCreateConsumerRecords()` - Actual poll
  - **Line 386**: `consumer.poll(Duration.ofSeconds(0L))` - Non-blocking poll
  - **Line 388-390**: Buffers results in queue

### 2.4 Deserialization
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- **Line 105-106**: `createConsumerRecord()` abstract method
  - Implemented by subclasses: `BinaryKafkaConsumerState`, `JsonKafkaConsumerState`, `SchemaKafkaConsumerState`
  - Converts Kafka format to client format and computes size

### 2.5 Offset Management
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java`
- **Line 272-300**: `commitOffsets()` method
  - **Line 276-299**: Async callback handling with `CommitCallback`

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerManager.java`
- **Line 475-509**: `commitOffsets()` method
  - **Line 489-508**: Submits offset commit task to executor

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- **Line 109-138**: `commitOffsets()` method
  - **Line 114-116**: `consumer.commitSync()` or `consumer.commitAsync()`
  - **Line 134**: `consumer.commitSync(offsetMap)` for specific offsets

### 2.6 Seeking Operations
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java`
- **Line 317-330**: `seekToBeginning()`
- **Line 332-346**: `seekToEnd()`
- **Line 348-362**: `seekToOffset()`

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- **Line 140-150**: `seekToBeginning()` - Calls `consumer.seekToBeginning()`
- **Line 152-162**: `seekToEnd()` - Calls `consumer.seekToEnd()`
- **Line 164-198**: `seek()` - Complex seeking with offsets and timestamps

### 2.7 Consumer Cleanup
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/ConsumersResource.java`
- **Line 120-127**: `deleteGroup()` method
  - Calls `KafkaConsumerManager.deleteConsumer()`

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- **Line 213-220**: `close()` method
  - **Line 215-219**: `consumer.close()` and cleanup

---

## 3. ADMIN OPERATIONS FLOW

### 3.1 Topic Listing
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicsResource.java`
- **Line 85-115**: `listTopics()` method
  - **Line 94-114**: Async response handling
  - Calls `TopicManager.listTopics()`

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/controllers/TopicManagerImpl.java`
- **Line 64-81**: `listTopics()` implementation
  - **Line 66-70**: Gets cluster, validates existence
  - **Line 70**: Calls `adminClient.listTopics().listings()`
  - **Line 76-79**: Calls `describeTopics()` to get full topic details

### 3.2 Topic Creation
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicsResource.java`
- **Line 168-249**: `createTopic()` method
  - **Line 178-180**: Validates request
  - **Line 182-187**: Validates topic name syntax
  - **Line 189-194**: Extracts partition config and replica assignments
  - **Line 240-248**: Calls `TopicManager.createTopic2()`

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/controllers/TopicManagerImpl.java`
- Uses `Admin` client (Apache Kafka AdminClient)
- Calls `adminClient.createTopics()` or `adminClient.alterTopics()` depending on operation

### 3.3 Topic Updating (Partition Count)
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicsResource.java`
- **Line 138-166**: `updatePartitionsCount()` method
  - **Line 156-163**: Calls `TopicManager.updateTopicPartitionsCount()`
  - Retrieves updated topic info after change

### 3.4 Broker Information
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/BrokersResource.java`
- **Line 62-88**: `listBrokers()` method
  - Calls `BrokerManager.listBrokers()`
  - Uses `adminClient.describeCluster()` to get broker metadata

### 3.5 Configuration Operations
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/TopicConfigsResource.java`
- Calls `TopicConfigManager` for config operations
- Uses `adminClient.describeConfigs()` and `adminClient.alterConfigs()`

---

## 4. ERROR FLOW

### 4.1 Error Type Hierarchy
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/Errors.java`

**Error Constants & Exception Builders**:
- **Line 30-35**: Kafka error codes (from parent KafkaExceptionMapper)
  - `KAFKA_AUTHENTICATION_ERROR_CODE = 40101`
  - `KAFKA_AUTHORIZATION_ERROR_CODE = 40102`
  - `KAFKA_RETRIABLE_ERROR_ERROR_CODE = 40003`
- **Line 37-51**: Topic/Partition not found errors
  - `TOPIC_NOT_FOUND_ERROR_CODE = 40401`
  - `PARTITION_NOT_FOUND_ERROR_CODE = 40402`
- **Line 53-114**: Consumer-related errors
  - `CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE = 40403`
  - `CONSUMER_GROUP_ID_NOT_FOUND_ERROR_CODE = 40405`
  - `CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE = 40901`
  - `ILLEGAL_STATE_ERROR_CODE = 40903`
- **Line 116-191**: Schema and serialization errors
  - `KEY_SCHEMA_MISSING_ERROR_CODE = 42201`
  - `VALUE_SCHEMA_MISSING_ERROR_CODE = 42202`
  - `SERIALIZATION_EXCEPTION_ERROR_CODE = 42207`
  - `INVALID_PAYLOAD_ERROR_CODE = 42206`
  - `JSON_CONVERSION_ERROR_CODE = 42203`

**Exception Creation Methods**:
- `topicNotFoundException()` (line 41)
- `consumerFormatMismatch()` (line 76)
- `messageSerializationException()` (line 181-191)
- `produceBatchException()` (line 204)
- `kafkaErrorException()` (line 214)

### 4.2 Exception Mapping - v3
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/v3/V3ExceptionMapper.java`
- **Line 24-32**: `toResponse()` implementation
  - Converts `StatusCodeException` to HTTP Response
  - Creates `ErrorResponse` with error code and message

**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/v3/ErrorResponse.java`
- Contains error code and message fields

### 4.3 Exception Mapping - v2
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/v2/V2ExceptionMapper.java`
- **Line 24-32**: Similar to v3, converts to v2 format

### 4.4 Kafka Exception Handling
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/KafkaRestExceptionMapper.java`
- **Line 31-39**: `toResponse()` method
  - Handles `SerializationException` specially (returns 408 Request Timeout with code 40801)
  - Delegates other Kafka exceptions to parent `KafkaExceptionMapper`

### 4.5 Producer Exception Handling (v2)
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v2/AbstractProduceAction.java`
- **Line 219-237**: `errorCodeFromProducerException()` method
  - **Line 220-221**: `AuthenticationException` → `KAFKA_AUTHENTICATION_ERROR_CODE`
  - **Line 222-223**: `AuthorizationException` → `KAFKA_AUTHORIZATION_ERROR_CODE`
  - **Line 224-225**: `RetriableException` → `KAFKA_RETRIABLE_ERROR_ERROR_CODE`
  - **Line 226-227**: `KafkaException` → `KAFKA_ERROR_ERROR_CODE`
  - **Line 228-236**: Non-Kafka exceptions → `RestServerErrorException`

### 4.6 Stackless Exceptions (Performance Optimization)
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/exceptions/StacklessCompletionException.java`
- **Line 29-43**: `StacklessCompletionException` class
  - **Line 41-43**: Overrides `fillInStackTrace()` to return `this` (prevents costly stack trace capture)
  - Used for rate limit exceptions and other non-critical errors in async paths

### 4.7 Producer Error Handling (v3)
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/resources/v3/ProduceAction.java`
- **Line 226-231**: Error handling in response composition
  - Catches exceptions and completes future exceptionally
  - Records error metrics at line 229

### 4.8 Consumer Exception Handling
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerManager.java`
- **Line 440-446**: Exception handling in read tasks
  - Logs error with consumer ID and task info
  - Invokes callback with exception
  - Consumer errors are caught in `RunnableReadTask.run()`

---

## KEY DATA STRUCTURES

### ProduceRequest (v3)
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/entities/v3/ProduceRequest.java`
- Contains: key, value, partition ID, headers, timestamp

### ProduceResponse (v3)
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/entities/v3/ProduceResponse.java`
- Contains: cluster ID, topic name, partition ID, offset, timestamp, key/value metadata, error code

### ProduceResult
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/entities/ProduceResult.java`
- Wraps Kafka `RecordMetadata`
- Contains: partition ID, offset, timestamp, serialized key/value sizes, completion timestamp

### KafkaConsumerState
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerState.java`
- **Line 65-88**: Generic type parameters for key/value transformation
- **Line 97-98**: Queue of consumer records for buffering

### KafkaConsumerReadTask
**File**: `/home/user/kafka-rest/kafka-rest/src/main/java/io/confluent/kafkarest/v2/KafkaConsumerReadTask.java`
- Manages single read request state
- Tracks: timeout, max/min bytes, messages collected, completion status

---

## THREADING MODEL

### Produce (v3)
- Async: HTTP request handled by JAX-RS async response
- Response formatting uses `ExecutorService` (ProduceResponseThreadPool)
- Producer callbacks are executed by Kafka producer's internal thread

### Consume (v2)
- **Thread Pool Executor**: `KafkaConsumerThreadPoolExecutor` (line 360 in KafkaConsumerManager)
  - Core pool size: 0
  - Max threads: configurable (default unlimited)
  - Queue: SynchronousQueue (no buffering)
- **Read Task Scheduling**: 
  - `ReadTaskSchedulerThread` manages re-scheduling of read tasks
  - Uses `DelayQueue` to manage backoff (line 103 in KafkaConsumerManager)
  - Tasks are re-queued with delay if no data available
- **Expiration Thread**: `ExpirationThread` periodically checks for expired consumer instances

### Admin Operations (v3)
- Async via `CompletableFuture`
- Kafka AdminClient handles async admin requests
- JAX-RS async response handles HTTP response continuation

---

## CONFIGURATION POINTS

**Producer Configuration**:
- Serializer format (BINARY, AVRO, JSON, JSONSCHEMA, PROTOBUF)
- Schema Registry connection
- Kafka producer properties

**Consumer Configuration**:
- Deserializer format (same as producer)
- Consumer group ID and instance name
- Request timeout
- Max bytes per request
- Response min bytes threshold

**Server Configuration**:
- Thread pool sizes
- Consumer request timeout (default 30000ms)
- Consumer iterator backoff (default 50ms)
- Consumer instance timeout (default 300000ms)

---

## END-TO-END FLOW SUMMARY

### Produce (v3):
HTTP POST → ProduceAction → Rate Limit Check → Schema Resolution → Serialization → ProduceController → KafkaProducer.send() → Callback → ProduceResult → Response Formatting → HTTP 200

### Consume (v2):
HTTP GET → ConsumersResource → KafkaConsumerManager → ThreadPool → KafkaConsumerReadTask → hasNext()/getOrCreateConsumerRecords() → consumer.poll(0) → Deserialization → AsyncCallback → HTTP 200

### Admin (v3):
HTTP GET/POST → Resource (TopicsResource/BrokersResource) → Manager (TopicManager/BrokerManager) → AdminClient → KafkaFutures → CompletableFuture → HTTP Response

### Error Handling:
Exception → ExceptionMapper → ErrorResponse JSON → HTTP Error Status

