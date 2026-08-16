# Specification

Financial Event Streaming Platform

Version 1.0
Status: Draft

---

## Purpose

This document defines the technical specification for the Financial Event Streaming Platform. It covers the event schema design, service interface contracts, Kafka configuration, observability instrumentation, scaling configuration, and deployment specifications. Engineers building any component of the platform should treat this document as the authoritative technical reference.

---

## Event Schema Specification

All events are serialised using Apache Avro. Schemas are registered in Confluent Schema Registry and versioned. The schema files live in `schemas/` at the repository root.

### TradeEvent

Topic: `trades.raw`

```json
{
  "namespace": "dev.engnotes.fes.events",
  "type": "record",
  "name": "TradeEvent",
  "doc": "Raw trade execution event from the trade producer",
  "fields": [
    {"name": "tradeId",       "type": "string",
     "doc": "Unique identifier for this trade execution"},
    {"name": "correlationId", "type": "string",
     "doc": "Cross-service trace identifier, propagated through pipeline"},
    {"name": "ticker",        "type": "string",
     "doc": "NSE/BSE ticker symbol e.g. RELIANCE, TCS"},
    {"name": "quantity",      "type": "long",
     "doc": "Number of shares traded"},
    {"name": "price",         "type": "double",
     "doc": "Execution price in INR"},
    {"name": "side",          "type": {"type": "enum", "name": "Side",
                                       "symbols": ["BUY", "SELL"]},
     "doc": "Trade direction"},
    {"name": "traderId",      "type": "string",
     "doc": "Identifier of the trader or algorithm placing the order"},
    {"name": "accountId",     "type": "string",
     "doc": "Account identifier for position tracking"},
    {"name": "eventTimestamp","type": "long",
     "logicalType": "timestamp-millis",
     "doc": "Unix timestamp in milliseconds when trade was executed"},
    {"name": "producedAt",    "type": "long",
     "logicalType": "timestamp-millis",
     "doc": "Unix timestamp when this event was produced to Kafka"},
    {"name": "traceContext",  "type": {"type": "map", "values": "string"},
     "default": {},
     "doc": "W3C TraceContext headers for distributed tracing"}
  ]
}
```

### EnrichedTradeEvent

Topic: `trades.enriched`

Extends TradeEvent with market data at execution time. The enrichment service adds the following fields to the original TradeEvent payload:

```json
{
  "namespace": "dev.engnotes.fes.events",
  "type": "record",
  "name": "EnrichedTradeEvent",
  "fields": [
    {"name": "trade",            "type": "TradeEvent",
     "doc": "Original trade event, preserved for downstream consumers"},
    {"name": "midPriceAtExecution","type": "double",
     "doc": "Mid-price (bid+ask)/2 at time of trade execution"},
    {"name": "spreadAtExecution", "type": "double",
     "doc": "Bid-ask spread in INR at time of trade execution"},
    {"name": "vwap5Min",          "type": "double",
     "doc": "Volume-weighted average price over last 5 minutes"},
    {"name": "marketCap",         "type": "double",
     "doc": "Market capitalisation of ticker in INR crores"},
    {"name": "priceDeviation",    "type": "double",
     "doc": "Percentage deviation of execution price from mid-price"},
    {"name": "enrichedAt",        "type": "long",
     "logicalType": "timestamp-millis",
     "doc": "Unix timestamp when enrichment completed"},
    {"name": "enrichmentLatencyMs","type": "long",
     "doc": "Time in milliseconds from consume to enriched publish"}
  ]
}
```

### RiskAlertEvent

Topic: `notifications.alerts`

```json
{
  "namespace": "dev.engnotes.fes.events",
  "type": "record",
  "name": "RiskAlertEvent",
  "fields": [
    {"name": "alertId",         "type": "string"},
    {"name": "correlationId",   "type": "string"},
    {"name": "triggeringTradeId","type": "string"},
    {"name": "alertType",       "type": {"type": "enum", "name": "AlertType",
                                          "symbols": [
                                            "POSITION_LIMIT_BREACH",
                                            "UNUSUAL_VOLUME",
                                            "PRICE_DEVIATION",
                                            "WASH_TRADE_DETECTED"
                                          ]}},
    {"name": "severity",        "type": {"type": "enum", "name": "Severity",
                                          "symbols": ["INFO", "WARNING", "CRITICAL"]}},
    {"name": "ticker",          "type": "string"},
    {"name": "traderId",        "type": "string"},
    {"name": "description",     "type": "string",
     "doc": "Human-readable description of the alert condition"},
    {"name": "ruleParameters",  "type": {"type": "map", "values": "string"},
     "doc": "Key-value pairs of the rule thresholds that triggered this alert"},
    {"name": "measuredValues",  "type": {"type": "map", "values": "string"},
     "doc": "Key-value pairs of the actual values that breached the thresholds"},
    {"name": "alertTimestamp",  "type": "long",
     "logicalType": "timestamp-millis"},
    {"name": "traceContext",    "type": {"type": "map", "values": "string"},
     "default": {}}
  ]
}
```

### DeadLetterEvent

Topic: `{source-topic}.dlq`

```json
{
  "namespace": "dev.engnotes.fes.events",
  "type": "record",
  "name": "DeadLetterEvent",
  "fields": [
    {"name": "originalTopic",    "type": "string"},
    {"name": "originalPartition","type": "int"},
    {"name": "originalOffset",   "type": "long"},
    {"name": "originalPayload",  "type": "bytes",
     "doc": "Raw Avro bytes of the original failed event"},
    {"name": "failureReason",    "type": "string"},
    {"name": "exceptionClass",   "type": "string"},
    {"name": "exceptionMessage", "type": "string"},
    {"name": "stackTraceSummary","type": "string",
     "doc": "First 500 characters of the stack trace"},
    {"name": "retryCount",       "type": "int"},
    {"name": "firstFailureAt",   "type": "long",
     "logicalType": "timestamp-millis"},
    {"name": "lastFailureAt",    "type": "long",
     "logicalType": "timestamp-millis"},
    {"name": "consumerGroup",    "type": "string"},
    {"name": "consumerInstance", "type": "string"}
  ]
}
```

---

## Kafka Configuration Specification

### Broker Configuration (MSK Production)

```properties
num.partitions=12
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
log.retention.hours=168
log.segment.bytes=536870912
log.retention.check.interval.ms=300000
compression.type=lz4
message.max.bytes=10485760
```

`min.insync.replicas=2` with `acks=all` on producers means a message is only acknowledged after it is written to at least 2 of 3 replicas. This prevents data loss when one broker fails. The cost is slightly higher producer latency.

`unclean.leader.election.enable=false` prevents a broker that fell behind from becoming leader. Without this, a lagging broker elected as leader can cause message loss. In a financial system this is non-negotiable.

### Producer Configuration

```properties
# Reliability (production)
acks=all
retries=2147483647
max.in.flight.requests.per.connection=5
enable.idempotence=true

# Performance
batch.size=65536
linger.ms=5
compression.type=lz4
buffer.memory=67108864

# Timeouts
request.timeout.ms=30000
delivery.timeout.ms=120000

# Serialisation
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
schema.registry.url=${SCHEMA_REGISTRY_URL}
```

For the load test simulator, `acks=1` is used instead of `acks=all` to maximise throughput. The simulator generates synthetic data where durability is not required. All production application producers use `acks=all`.

### Consumer Configuration

```properties
# Offset management
enable.auto.commit=false
auto.offset.reset=earliest
isolation.level=read_committed

# Performance
fetch.min.bytes=1
fetch.max.wait.ms=500
max.poll.records=500
max.poll.interval.ms=300000

# Session management
session.timeout.ms=45000
heartbeat.interval.ms=15000

# Serialisation
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
schema.registry.url=${SCHEMA_REGISTRY_URL}
specific.avro.reader=true
```

`enable.auto.commit=false` requires the consumer to commit offsets explicitly after processing. Spring Kafka handles this in `MANUAL_IMMEDIATE` acknowledge mode. This ensures that a consumer crash before commit results in reprocessing, not message loss.

`isolation.level=read_committed` ensures consumers only read messages from committed transactions. This matters when producers use transactional APIs, which is the case for exactly-once delivery paths.

---

## Service Interface Specification

### Trade Enrichment Service

Consumer group: `trade-enrichment-service`
Input topic: `trades.raw`
Output topic: `trades.enriched`
DLQ topic: `trades.raw.dlq`

Spring Kafka listener configuration:

```java
@KafkaListener(
    topics = "trades.raw",
    groupId = "trade-enrichment-service",
    containerFactory = "enrichmentListenerFactory"
)
public void consume(
    ConsumerRecord<String, TradeEvent> record,
    Acknowledgment acknowledgment
) {
    // Process event
    // On success: publish to trades.enriched, acknowledge offset
    // On retriable failure: throw RetriableException (Spring retries)
    // On non-retriable failure: publish to DLQ, acknowledge offset
}
```

Retry policy in Spring Kafka:

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, DeadLetterEvent> dlqTemplate) {
    BackOff backOff = new ExponentialBackOff(100L, 2.0);
    backOff.setMaxElapsedTime(5000L); // max 5 seconds total retry time

    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(dlqTemplate,
            (record, ex) -> new TopicPartition(
                record.topic() + ".dlq",
                record.partition()
            )
        );

    return new DefaultErrorHandler(recoverer, backOff);
}
```

### Risk Alert Service

Consumer group: `risk-alert-service`
Input topic: `trades.enriched`
Output topic: `notifications.alerts`

Rule engine interface:

```java
public interface RiskRule {
    String getRuleName();
    boolean evaluate(EnrichedTradeEvent event, RiskContext context);
    RiskAlertEvent buildAlert(EnrichedTradeEvent event, RiskContext context);
}
```

Rule configuration in `application.yml`:

```yaml
risk:
  rules:
    position-limit:
      enabled: true
      threshold-shares: 100000
    unusual-volume:
      enabled: true
      std-deviation-threshold: 3.0
      rolling-window-minutes: 60
    price-deviation:
      enabled: true
      max-deviation-percent: 2.0
    wash-trade:
      enabled: true
      detection-window-seconds: 60
```

### Audit Service

Consumer group: `audit-service`
Input topics: `trades.raw`, `trades.enriched`, `market-data.ticks`, `corporate-actions`, `notifications.alerts`

S3 write specification:

```java
public class AuditWriter {
    // Buffer configuration
    private static final int BUFFER_SIZE = 10_000;
    private static final Duration FLUSH_INTERVAL = Duration.ofSeconds(30);

    // S3 key format
    private String buildS3Key(String eventType, Instant timestamp) {
        return String.format(
            "year=%d/month=%02d/day=%02d/event_type=%s/%s-%s.parquet",
            timestamp.atZone(ZoneOffset.UTC).getYear(),
            timestamp.atZone(ZoneOffset.UTC).getMonthValue(),
            timestamp.atZone(ZoneOffset.UTC).getDayOfMonth(),
            eventType,
            timestamp.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("HH-mm-ss")),
            UUID.randomUUID().toString().substring(0, 8)
        );
    }
}
```

### DLQ Replay API

REST endpoint on the notification service for operator use:

```
POST /api/v1/dlq/replay
Content-Type: application/json

{
  "sourceTopic": "trades.raw.dlq",
  "targetTopic": "trades.raw",
  "maxMessages": 100,
  "filterByCorrelationId": "optional-correlation-id"
}

Response 200:
{
  "replayed": 47,
  "skipped": 0,
  "failed": 0,
  "targetTopic": "trades.raw",
  "replayedAt": "2026-06-28T10:00:00Z"
}
```

---

## Observability Specification

### OpenTelemetry Collector Configuration

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 1s
    send_batch_size: 1024
  memory_limiter:
    check_interval: 1s
    limit_mib: 512

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true
  prometheus:
    endpoint: 0.0.0.0:8889
  loki:
    endpoint: http://loki:3100/loki/api/v1/push

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [jaeger]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [prometheus]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [loki]
```

### Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'trade-enrichment-service'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets: ['trade-enrichment-service:8080']

  - job_name: 'risk-alert-service'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets: ['risk-alert-service:8080']

  - job_name: 'audit-service'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets: ['audit-service:8080']

  - job_name: 'kafka-exporter'
    scrape_interval: 15s
    static_configs:
      - targets: ['kafka-exporter:9308']
```

### Structured Log Format

Every log line must be valid JSON with the following structure:

```json
{
  "timestamp": "2026-06-28T10:15:30.123Z",
  "level": "INFO",
  "service": "trade-enrichment-service",
  "instance": "trade-enrichment-service-7d4f8b9c6-x2p4k",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "spanId": "00f067aa0ba902b7",
  "topic": "trades.raw",
  "partition": 3,
  "offset": 12847,
  "eventType": "TradeEvent",
  "ticker": "RELIANCE",
  "processingLatencyMs": 4,
  "cacheHit": true,
  "environment": "dev",
  "message": "Trade enrichment complete"
}
```

Logback configuration for JSON output:

```xml
<dependency>
    <groupId>net.logstash.logback</groupId>
    <artifactId>logstash-logback-encoder</artifactId>
    <version>7.4</version>
</dependency>
```

```xml
<appender name="JSON" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="net.logstash.logback.encoder.LogstashEncoder">
        <customFields>
            {"service":"${SERVICE_NAME}","environment":"${ENVIRONMENT}"}
        </customFields>
    </encoder>
</appender>
```

---

## KEDA Specification

### ScaledObject for Trade Enrichment Service

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: trade-enrichment-scaler
  namespace: fes
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: trade-enrichment-service
  pollingInterval: 30
  cooldownPeriod: 120
  minReplicaCount: 1
  maxReplicaCount: 20
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.monitoring.svc:9090
      metricName: kafka_consumer_lag_sum
      query: |
        sum(kafka_consumer_lag_by_partition{
          group="trade-enrichment-service"
        })
      threshold: "500"
      activationThreshold: "10"
```

`activationThreshold: "10"` means KEDA does not scale out until lag exceeds 10. This prevents unnecessary scaling on brief traffic spikes.

`cooldownPeriod: 120` means KEDA waits 120 seconds after lag drops below threshold before scaling in. This prevents thrashing where instances are created and destroyed repeatedly during variable load.

### ScaledObject for Risk Alert Service

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: risk-alert-scaler
  namespace: fes
spec:
  scaleTargetRef:
    name: risk-alert-service
  pollingInterval: 30
  cooldownPeriod: 180
  minReplicaCount: 1
  maxReplicaCount: 12
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.monitoring.svc:9090
      metricName: kafka_consumer_lag_sum
      query: |
        sum(kafka_consumer_lag_by_partition{
          group="risk-alert-service"
        })
      threshold: "200"
```

The Risk Alert Service has a lower threshold (200 vs 500) because risk evaluation latency is more sensitive. A large lag means risk alerts are delayed, which is more serious than enrichment delay.

---

## Load Test Specification

### k6 Script Structure

```
load-tests/
  k6/
    scenarios/
      ramp-to-10k.js      ramp from 0 to 10,000 events/sec
      sustain-10k.js      hold at 10,000 events/sec for 5 minutes
      spike-to-50k.js     spike from 10,000 to 50,000 events/sec
      full-lifecycle.js   ramp, sustain, spike, drain sequence
    helpers/
      metrics.js          custom k6 metrics for financial events
      thresholds.js       pass/fail thresholds
```

### Full Lifecycle Load Test

```javascript
import { check } from 'k6';

export const options = {
  scenarios: {
    ramp_up: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      stages: [
        { target: 10000, duration: '2m' },   // ramp to 10k/sec
        { target: 10000, duration: '5m' },   // sustain at 10k/sec
        { target: 50000, duration: '30s' },  // spike to 50k/sec
        { target: 50000, duration: '3m' },   // sustain at 50k/sec
        { target: 0,     duration: '1m' },   // drain
      ],
      preAllocatedVUs: 100,
      maxVUs: 500,
    }
  },
  thresholds: {
    'pipeline_e2e_latency_ms': ['p(99)<200'],      // end-to-end p99 < 200ms
    'enrichment_latency_ms':   ['p(99)<10'],       // enrichment p99 < 10ms
    'risk_eval_latency_ms':    ['p(99)<5'],        // risk eval p99 < 5ms
    'dlq_rate':                ['rate<0.001'],     // less than 0.1% to DLQ
    'error_rate':              ['rate<0.001'],     // less than 0.1% errors
  }
};
```

### Pass/Fail Criteria

The load test passes and results are committed to `results/` if all of the following are met:

At 10,000 events per second sustained load:
- p99 end-to-end latency below 200ms
- p99 enrichment latency below 10ms
- p99 risk evaluation latency below 5ms
- DLQ rate below 0.1 percent
- Zero consumer group rebalances during the sustained phase

At 50,000 events per second spike:
- Consumer lag stabilises within 90 seconds of spike onset
- KEDA scales out to at least 6 instances within 90 seconds
- No message loss (verified by comparing producer count to audit archive count)
- System returns to nominal lag within 4 minutes of spike end

---

## ECS Task Definition Specification

### Trade Enrichment Service Task

```json
{
  "family": "trade-enrichment-service",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "taskRoleArn": "arn:aws:iam::ACCOUNT:role/fes-trade-enrichment-task-role",
  "executionRoleArn": "arn:aws:iam::ACCOUNT:role/fes-ecs-execution-role",
  "containerDefinitions": [
    {
      "name": "trade-enrichment-service",
      "image": "ACCOUNT.dkr.ecr.ap-south-1.amazonaws.com/fes/trade-enrichment:latest",
      "essential": true,
      "portMappings": [
        {"containerPort": 8080, "protocol": "tcp"}
      ],
      "environment": [
        {"name": "SPRING_PROFILES_ACTIVE", "value": "ecs"},
        {"name": "ENVIRONMENT", "value": "production"},
        {"name": "SERVICE_NAME", "value": "trade-enrichment-service"}
      ],
      "secrets": [
        {
          "name": "KAFKA_BOOTSTRAP_SERVERS",
          "valueFrom": "arn:aws:secretsmanager:ap-south-1:ACCOUNT:secret:fes/kafka-bootstrap-servers"
        },
        {
          "name": "SCHEMA_REGISTRY_URL",
          "valueFrom": "arn:aws:secretsmanager:ap-south-1:ACCOUNT:secret:fes/schema-registry-url"
        },
        {
          "name": "REDIS_URL",
          "valueFrom": "arn:aws:secretsmanager:ap-south-1:ACCOUNT:secret:fes/redis-url"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/fes/trade-enrichment-service",
          "awslogs-region": "ap-south-1",
          "awslogs-stream-prefix": "ecs"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL",
          "curl -f http://localhost:8080/actuator/health || exit 1"],
        "interval": 30,
        "timeout": 5,
        "retries": 3,
        "startPeriod": 60
      }
    }
  ]
}
```

---

## Module Delivery Checklist

Each module is considered complete when all of the following are true:

Code compiles and all unit tests pass in CI.

Integration tests pass against embedded Kafka (using `spring-kafka-test`).

Docker image builds successfully and runs in Docker Compose.

A manual smoke test confirms the happy path end-to-end.

Custom Prometheus metrics are visible in Grafana.

The module's contribution to the README is committed.

For modules that introduce Avro schemas: schema compatibility check passes in CI.

For modules that introduce new services: the service appears in the pipeline health Grafana dashboard.

For Module 7 (load test): results files are committed to `results/` with the run timestamp, event counts, and p50/p95/p99 latency for each phase.

For Module 8 (EKS): screenshots of KEDA scaling, Grafana dashboards, and the X-Ray or Jaeger service map are committed to `docs/screenshots/` before the cluster is destroyed.
