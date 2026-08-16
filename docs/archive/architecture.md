# Architecture

Financial Event Streaming Platform

Version 1.0
Status: Draft

---

## Overview

The Financial Event Streaming Platform is a Kafka-based event streaming system for financial markets. It ingests trade executions, market data ticks, and corporate actions, processes them through enrichment and risk evaluation pipelines, and archives every event to an immutable S3-based audit trail.

The architecture follows three principles. First, every component is independently scalable: producers, consumers, and the observability stack scale without coordination. Second, observability is a first-class design concern: every service emits OpenTelemetry traces, Prometheus metrics, and structured logs from day one. Third, failure is expected: dead letter queues, retry policies, and circuit breakers are designed in, not added later.

---

## System Context

```
External                Platform                    Storage
--------                --------                    -------

Market Data    ------>  Market Data Producer  ---->  Kafka
Simulator               (GBM price model)

Trade          ------>  Trade Producer        ---->  Kafka
Simulator               (Pareto volumes)

Corporate      ------>  Corporate Action      ---->  Kafka
Events                  Producer

                        Trade Enrichment  <----  Kafka
                        Service           ---->  Kafka (enriched)
                                          <-->   Redis (cache)

                        Risk Alert        <----  Kafka (enriched)
                        Service           ---->  Kafka (alerts)
                                          <-->   PostgreSQL (positions)

                        Audit Service     <----  Kafka (all topics)
                                          ---->  S3 (Parquet)

                        Notification      <----  Kafka (alerts)
                        Service           ---->  Downstream systems

Operators  <---------   Grafana / Prometheus / Loki / Jaeger
```

---

## Component Architecture

### Kafka Cluster

Three brokers in local Docker Compose. Amazon MSK with three brokers across three availability zones in ECS and EKS deployments.

Topic design: 12 partitions per topic to allow scaling to 12 consumer instances per consumer group. Replication factor of 3 in cloud deployments, 1 in local.

| Topic | Purpose | Retention | Partitions |
|---|---|---|---|
| `trades.raw` | Raw trade execution events | 7 days | 12 |
| `trades.enriched` | Enriched trade events | 7 days | 12 |
| `market-data.ticks` | Price tick stream | 1 day | 12 |
| `corporate-actions` | Corporate action events | 30 days | 6 |
| `notifications.alerts` | Risk alerts | 3 days | 6 |
| `trades.raw.dlq` | Failed enrichment events | 30 days | 6 |
| `trades.enriched.dlq` | Failed risk evaluation events | 30 days | 6 |

12 partitions is the scaling ceiling for each consumer group without partition reassignment. This number is chosen deliberately: it allows KEDA to scale to 12 instances before partitions become the bottleneck.

### Schema Registry

Confluent Schema Registry stores Avro schemas with backward compatibility enforcement. Producers validate schemas at startup. A CI job runs schema compatibility checks against the registry on every pull request before merge.

Schema evolution policy: backward compatible changes only in the main branch. New optional fields with defaults. No field removal. No type changes. Breaking changes require a new topic version.

### Market Data Simulator

A Spring Boot service that generates statistically plausible financial events for load testing. It does not call any external API. All data is generated in-process.

Price simulation uses Geometric Brownian Motion:

```
dS = S * (mu * dt + sigma * dW)

where:
  S     = current price
  mu    = drift parameter (configurable, default 8% annualised)
  sigma = volatility parameter (configurable, default 20% annualised)
  dt    = time step (1 second expressed in trading years)
  dW    = Wiener process increment (standard normal sample)
```

Volume simulation uses a Pareto distribution with shape parameter 1.5, reproducing the empirically observed heavy-tailed distribution of trade sizes in real markets.

The simulator runs in two modes. Realistic mode generates events at market-hours frequency with higher rates at open and close. Load test mode generates maximum throughput for a configurable duration, targeting up to 50,000 events per second across all tickers.

At 50,000 events per second, each Kafka producer batch contains approximately 500 events flushed every 10ms. Producer configuration for maximum throughput:

```properties
batch.size=65536
linger.ms=5
compression.type=lz4
acks=1
buffer.memory=67108864
```

`acks=1` is intentional for load testing: we are measuring consumer pipeline performance, not producer durability. Production deployments use `acks=all`.

### Trade Enrichment Service

Consumes from `trades.raw`. For each event: retrieves current market data from Redis cache, computes derived fields, publishes enriched event to `trades.enriched`.

Cache miss handling: on a Redis cache miss, the service calls the market data simulator's HTTP endpoint to get a current price. This adds latency but guarantees enrichment can proceed. Cache miss rate is monitored as a custom Prometheus metric.

Retry policy: 3 attempts with exponential backoff starting at 100ms. After the third failure, the event is published to `trades.raw.dlq` with full failure context.

Poison pill detection: a circuit breaker per event type tracks consecutive failures. If the same event ID fails 3 times across restarts, it is quarantined in the DLQ and the circuit breaker blocks further processing of that event type for 60 seconds.

Consumer group: `trade-enrichment-service`. One consumer group, 12 partitions, scales from 1 to 12 instances via KEDA.

### Risk Alert Service

Consumes from `trades.enriched`. Evaluates each trade against a configurable rule engine. Rules are loaded from a YAML configuration file and refreshed at runtime via Spring Cloud Config or a ConfigMap in Kubernetes.

Rule evaluation is stateful: position tracking requires a running total per trader per ticker. State is maintained in PostgreSQL with optimistic locking. The service uses a consistent hash of the ticker symbol to route events to the same partition, ensuring the same consumer instance handles all trades for a given ticker and avoids cross-instance state coordination.

Alert routing: CRITICAL alerts are published to `notifications.alerts` with immediate flush. WARNING and INFO alerts are batched and published every 5 seconds.

Consumer group: `risk-alert-service`.

### Audit Service

Consumes from all topics using a single consumer group: `audit-service`. Buffers events in memory for 30 seconds or until the buffer reaches 10,000 events, then writes a Parquet file to S3.

S3 key format: `year=YYYY/month=MM/day=DD/event_type=TYPE/HH-MM-SS-UUID.parquet`

Parquet was chosen over JSON for column-level compression and Athena query efficiency. A query for all CRITICAL risk alerts on a given day scans one date partition and one event type partition, not the full dataset.

The Audit Service uses the `at-least-once` delivery guarantee. Downstream consumers of the S3 audit data must handle duplicates. Idempotency key: combination of topic, partition, and offset is unique per event and can be used for deduplication in analytics queries.

### Notification Service

Consumes from `notifications.alerts`. In this implementation, the notification service logs structured alert records and exposes them via a REST endpoint for inspection. In a production extension, this service would dispatch to PagerDuty, Slack, or SMS.

---

## Observability Architecture

Observability is not a layer added after the services are built. It is designed into each service from the first commit.

### OpenTelemetry

The OpenTelemetry Java agent is attached to every service via the `JAVA_TOOL_OPTIONS` environment variable. Auto-instrumentation covers Spring Boot, Kafka client, JDBC, and Redis.

Manual instrumentation adds business-level spans around:

- Event deserialization and Avro schema resolution
- Cache lookup and cache miss fallback
- Rule evaluation in the Risk Alert Service
- S3 write operations in the Audit Service

Trace context propagation: the producer injects W3C TraceContext headers into Kafka message headers at produce time. Consumers extract these headers and create child spans. A single trade event produces a trace tree showing the complete lifecycle across four services.

The OpenTelemetry Collector receives traces, metrics, and logs from all services. It exports traces to Jaeger (local) or AWS X-Ray (cloud), metrics to Prometheus, and logs to Loki.

### Prometheus

Prometheus scrapes metrics from all services every 15 seconds. Custom metrics in addition to the JVM and Spring Boot defaults:

```
# Consumer lag
kafka_consumer_lag_by_partition{topic, partition, group}

# Processing latency
event_processing_duration_seconds{service, event_type, status}
  Buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 200ms, 500ms, 1000ms

# Throughput
events_processed_total{service, topic, status}

# DLQ
dlq_events_published_total{topic, failure_reason}
dlq_current_depth{topic}

# Cache
cache_hit_total{service, cache_name}
cache_miss_total{service, cache_name}
cache_miss_latency_seconds{service}

# Business
risk_alerts_fired_total{alert_type, severity}
trades_enriched_total{status}
audit_records_written_total{event_type}
```

KEDA reads `kafka_consumer_lag_by_partition` directly from Prometheus to make scaling decisions. The metric must be available and accurate for autoscaling to function.

### Grafana

Three dashboards committed as JSON under `observability/grafana/dashboards/`.

Pipeline health dashboard: consumer lag per partition (heatmap), throughput per topic (time series), error rate per service (gauge), DLQ depth per topic (stat), KEDA-managed replica count per service (time series). This is the dashboard open during deployments and incidents.

Service latency dashboard: p50, p95, p99 processing time per event type per service (time series), latency histogram distribution at current moment (heatmap), top 10 slowest event types (table), cache miss rate (gauge). This is the dashboard used during performance investigations.

Business signals dashboard: trades processed per minute (time series), risk alerts by severity over time (stacked bar), corporate actions processed (counter), audit lag (the time between event production and S3 availability), DLQ events by failure reason (pie chart). This is the dashboard a business stakeholder reads.

Alert rules in `observability/prometheus/alerts.yml`:

```yaml
- alert: HighConsumerLag
  expr: kafka_consumer_lag_by_partition > 1000
  for: 60s
  labels:
    severity: warning

- alert: HighErrorRate
  expr: rate(events_processed_total{status="failure"}[5m]) /
        rate(events_processed_total[5m]) > 0.01
  for: 5m
  labels:
    severity: critical

- alert: DlqDepthHigh
  expr: dlq_current_depth > 100
  for: 30s
  labels:
    severity: warning

- alert: ServiceDown
  expr: up{job=~"trade-enrichment|risk-alert|audit"} == 0
  for: 30s
  labels:
    severity: critical
```

### Loki and Structured Logging

All services log JSON to stdout. The Docker Compose and ECS configurations ship stdout to Loki via the OpenTelemetry Collector log pipeline.

Every log line includes: `timestamp`, `level`, `service`, `correlationId`, `traceId`, `spanId`, `topic`, `partition`, `offset`, `processingLatencyMs`, `eventType`, `environment`.

Useful Loki queries:

Find all log lines for a single trade event:
```
{service=~".+"} | json | correlationId = "abc-123"
```

Find all DLQ events in the last hour:
```
{service="trade-enrichment-service"} | json | level = "ERROR" | dlq = "true"
```

Find slow events (processing latency above 100ms):
```
{service=~".+"} | json | processingLatencyMs > 100
```

---

## Autoscaling Architecture

KEDA is deployed as a cluster-level operator. Each consumer service has a ScaledObject that defines the scaling behaviour.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: trade-enrichment-scaler
spec:
  scaleTargetRef:
    name: trade-enrichment-service
  minReplicaCount: 1
  maxReplicaCount: 20
  cooldownPeriod: 120
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      metricName: kafka_consumer_lag_by_partition
      query: |
        sum(kafka_consumer_lag_by_partition{
          group="trade-enrichment-service"
        })
      threshold: "500"
```

KEDA evaluates the trigger query every 30 seconds. When the total lag across all partitions exceeds 500, it increases the replica count. When lag drops below 50 for 120 seconds (cooldownPeriod), it scales down.

The relationship between lag and replica count is linear: KEDA targets one replica per 500 messages of lag, up to the partition count. With 12 partitions and a lag of 6,000, KEDA targets 12 replicas.

---

## Deployment Architecture

### Local (Docker Compose)

All infrastructure components run in Docker Compose. Services are built as container images and run alongside the infrastructure. The `docker compose up` command brings up the complete stack in approximately 90 seconds on a machine with 16GB RAM.

```
Services:        kafka-1, kafka-2, kafka-3
                 zookeeper
                 schema-registry
                 kafka-ui
                 postgresql
                 redis
                 prometheus
                 grafana
                 loki
                 otel-collector
                 jaeger

Application:     market-data-simulator
                 trade-producer
                 corporate-action-producer
                 trade-enrichment-service
                 risk-alert-service
                 audit-service
                 notification-service
```

### ECS Fargate

Amazon MSK replaces the self-managed Kafka brokers. Each consumer service runs as an ECS service with Fargate launch type.

Task definitions are managed by AWS CDK. Each task has a dedicated IAM task role with least-privilege access to the resources it needs: Secrets Manager for credentials, S3 for audit writes, MSK for Kafka access, CloudWatch for metrics and logs.

ECS Service Auto Scaling based on CPU and memory provides a second layer of scaling below KEDA. KEDA operates at the Kubernetes level for EKS. For ECS, KEDA integration with ECS is used via the KEDA HTTP add-on or custom metrics.

Fargate Spot is used for the simulator and non-critical consumer services to reduce cost by up to 70 percent. The Audit Service runs on regular Fargate to avoid interruption during S3 writes.

### EKS

Kubernetes manifests are in `k8s/` and wrapped in a Helm chart. The chart supports three value files: `values-dev.yaml`, `values-staging.yaml`, `values-prod.yaml`.

KEDA is installed as a cluster add-on. ScaledObjects are deployed alongside the consumer services.

The EKS deployment is validated once using a single node group with 3 `m5.xlarge` instances, the load test is run at 50,000 events per second, screenshots are taken, and the cluster is destroyed. Estimated validation cost: $10 to $15 USD for a weekend run.

---

## Data Flow

A complete trade event lifecycle from production to audit:

1. Trade Producer generates a TradeEvent in Avro format with a new correlationId and injects W3C trace context into message headers.

2. Kafka receives the message on `trades.raw` partition determined by hash of ticker symbol.

3. Trade Enrichment Service consumer reads the message, extracts trace context, creates a child span. Looks up current market data from Redis. If cache hit, enrichment completes in under 2ms. Publishes EnrichedTradeEvent to `trades.enriched`.

4. Risk Alert Service consumer reads from `trades.enriched`. Evaluates position limit, volume anomaly, and price deviation rules. If no breach, publishes nothing. If breach, publishes RiskAlertEvent to `notifications.alerts` with CRITICAL severity.

5. Audit Service consumer reads from all topics concurrently. Buffers the TradeEvent, EnrichedTradeEvent, and RiskAlertEvent. After 30 seconds, writes a Parquet file to S3.

6. Throughout steps 1 to 5, OpenTelemetry creates spans at each stage. The complete trace is available in Jaeger showing end-to-end latency breakdown.

7. Grafana shows the consumer lag spike from the 50,000 events per second load test and the KEDA scale-out from 2 to 12 instances.

Total latency from step 1 to risk alert available on the notification topic: under 200ms at p99 under nominal load of 10,000 events per second.

---

## Architecture Decision Records

The following decisions are documented in full in `docs/adr/`:

ADR-001: MSK over self-managed Kafka for cloud deployments.
ADR-002: Avro over JSON for event serialisation.
ADR-003: KEDA over HPA for Kafka consumer autoscaling.
ADR-004: ECS Fargate over EC2 for container deployment.
ADR-005: Parquet over JSON for audit storage.
ADR-006: GBM model for synthetic market data generation.
ADR-007: Redis over in-memory cache for market data.
ADR-008: PostgreSQL over DynamoDB for risk position state.

---

## Technology Stack

| Layer | Technology | Version | Rationale |
|---|---|---|---|
| Language | Java | 25 | LTS, virtual threads, AOT caches |
| Framework | Spring Boot | 3.4 | Spring Kafka, Actuator, Cloud Config |
| Messaging | Apache Kafka | 3.7 | Industry standard, MSK compatible |
| Schema | Avro + Schema Registry | 7.6 | Backward compatibility enforcement |
| Cache | Redis | 7.2 | Sub-millisecond market data lookup |
| Database | PostgreSQL | 16 | Risk position state with ACID guarantees |
| Storage | S3 + Parquet | - | Cost-efficient audit archive |
| Tracing | OpenTelemetry | 2.x | Vendor-neutral, auto-instrumentation |
| Metrics | Prometheus | 2.x | KEDA integration, Grafana data source |
| Logging | Loki | 3.x | Log aggregation with label filtering |
| Dashboards | Grafana | 11.x | Dashboard-as-code JSON |
| Autoscaling | KEDA | 2.x | Kafka lag-based scaling |
| IaC (ECS) | AWS CDK | 2.x | Java-native infrastructure |
| IaC (EKS) | Helm | 3.x | Kubernetes packaging standard |
| CI | GitHub Actions | - | Build, test, schema check |
| Kafka (cloud) | Amazon MSK | - | Managed brokers, no operational overhead |
| Compute (cloud) | ECS Fargate | - | Serverless containers, no node management |
