# Requirements

Financial Event Streaming Platform

Version 1.0
Status: Draft

---

## Purpose

This document defines the functional and non-functional requirements for a production-grade Kafka-based financial event streaming platform. The platform ingests real-time financial events, processes them through enrichment and risk evaluation pipelines, and archives them to an immutable audit trail. It is designed to demonstrate Principal Architect-level capabilities in distributed streaming systems, observability, and auto-scaling on AWS.

---

## Scope

The platform covers the following boundaries:

In scope: event production, schema management, consumer pipeline services, dead letter handling, observability stack, KEDA-based autoscaling, ECS Fargate deployment, EKS manifests.

Out of scope: order management systems, trade settlement, regulatory reporting to SEBI, authenticated market data feeds from licensed providers.

---

## Stakeholders

| Stakeholder | Interest |
|---|---|
| Platform engineers | Build and operate the streaming pipeline |
| Risk team | Consume real-time alerts for position breach and anomalies |
| Compliance team | Access immutable audit trail for regulatory review |
| Operations team | Monitor pipeline health via dashboards and alerts |

---

## Functional Requirements

### FR-01: Event Production

The platform must produce three categories of financial events to Kafka topics.

FR-01.1: The Trade Producer must publish trade execution events to the `trades.raw` topic. Each event must include trade ID, ticker symbol, quantity, price, side (buy or sell), trader ID, timestamp, and correlation ID.

FR-01.2: The Market Data Producer must publish price tick events to the `market-data.ticks` topic at configurable rates. Each event must include ticker symbol, bid price, ask price, last traded price, volume, and timestamp.

FR-01.3: The Corporate Action Producer must publish corporate action events to the `corporate-actions` topic. Supported action types: dividend declaration, stock split, earnings announcement, rights issue.

FR-01.4: In load simulation mode, the Market Data Simulator must sustain a configurable production rate from 1,000 to 50,000 events per second using a Geometric Brownian Motion price model and Pareto-distributed volume.

FR-01.5: All producers must inject OpenTelemetry trace context into Kafka message headers to enable end-to-end distributed tracing.

### FR-02: Schema Management

FR-02.1: All events must be serialised using Apache Avro with schemas registered in Confluent Schema Registry.

FR-02.2: Schema evolution must be validated in CI. A schema change that breaks backward compatibility must fail the pipeline before merge.

FR-02.3: The platform must support at least two schema versions active simultaneously during rolling upgrades.

### FR-03: Trade Enrichment Service

FR-03.1: The service must consume from `trades.raw` and enrich each trade with current market data retrieved from Redis cache.

FR-03.2: Enrichment must add: mid-price at execution time, spread at execution time, volume-weighted average price for the ticker over the last 5 minutes, and market capitalisation.

FR-03.3: Enriched events must be published to `trades.enriched`.

FR-03.4: If enrichment fails after 3 retry attempts with exponential backoff, the event must be published to `trades.raw.dlq` with the failure reason, original payload, retry count, and timestamp of first failure.

FR-03.5: The service must detect and handle poison pill events: events that fail consistently regardless of retry must be quarantined in the DLQ and must not block the consumer.

### FR-04: Risk Alert Service

FR-04.1: The service must consume from `trades.enriched` and evaluate each trade against configurable risk rules.

FR-04.2: The following risk rules must be supported:

Position limit breach: alert when a single trader's net position in a ticker exceeds a configurable threshold.

Unusual volume: alert when trade volume exceeds 3 standard deviations above the rolling 60-minute mean for that ticker.

Price deviation: alert when execution price deviates more than 2 percent from the last market data tick.

Wash trade detection: alert when buy and sell of the same ticker by related accounts occur within 60 seconds.

FR-04.3: Risk alerts must be published to `notifications.alerts` with severity (INFO, WARNING, CRITICAL), alert type, triggering trade ID, and evaluated rule parameters.

FR-04.4: Rules must be configurable at runtime without service restart.

### FR-05: Audit Service

FR-05.1: The service must consume from all topics and write every event to S3 in Parquet format.

FR-05.2: S3 keys must be partitioned by year, month, day, and event type to support efficient Athena queries.

FR-05.3: Audit records must be immutable. No update or delete operations are permitted on audit data.

FR-05.4: The service must guarantee at-least-once delivery to S3. Duplicate events in the audit trail are acceptable. Missing events are not.

FR-05.5: Audit records must be queryable via Athena within 5 minutes of event production.

### FR-06: Dead Letter Queue Management

FR-06.1: Every primary topic must have a corresponding DLQ topic with the naming convention `{topic}.dlq`.

FR-06.2: DLQ events must include: original topic, original partition, original offset, failure reason, exception class, stack trace summary, retry count, first failure timestamp, last failure timestamp.

FR-06.3: The platform must expose an API endpoint to replay events from a DLQ back to the original topic after manual inspection.

FR-06.4: DLQ depth must be monitored and an alert must fire when any DLQ exceeds 100 unprocessed messages.

### FR-07: Observability

FR-07.1: All services must emit OpenTelemetry traces with spans for Kafka produce, Kafka consume, external cache access, and database access.

FR-07.2: All services must expose Prometheus metrics on a `/actuator/prometheus` endpoint.

FR-07.3: The following custom metrics must be present in every consumer service:

`kafka_consumer_lag_by_partition` with labels for topic, partition, and consumer group.

`event_processing_duration_seconds` histogram with labels for service and event type.

`events_processed_total` counter with labels for service, topic, and status (success, failure, dlq).

`dlq_events_total` counter with labels for topic and failure reason.

FR-07.4: Three Grafana dashboards must be committed to the repository as JSON: pipeline health, service latency, and business signals.

FR-07.5: Alert rules must be defined in Prometheus for: consumer lag above 1,000 per partition for more than 60 seconds, error rate above 1 percent over 5 minutes, DLQ depth above 100, service instance count at zero.

FR-07.6: All log lines must be structured JSON with the following mandatory fields: timestamp, level, service, correlationId, traceId, spanId, topic, partition, offset, processingLatencyMs.

### FR-08: Autoscaling

FR-08.1: KEDA must be deployed and configured to scale consumer deployments based on Kafka consumer lag.

FR-08.2: Scale-out trigger: consumer lag above 500 per partition, sustained for 30 seconds.

FR-08.3: Scale-in trigger: consumer lag below 50 per partition, sustained for 120 seconds.

FR-08.4: Minimum replicas: 1. Maximum replicas: 20 per service.

FR-08.5: Scale-out must produce measurable lag reduction within 90 seconds of new instances becoming ready.

### FR-09: Deployment

FR-09.1: A Docker Compose file must bring up the complete local stack including Kafka, Schema Registry, Kafka UI, PostgreSQL, Redis, Prometheus, Grafana, Loki, and OpenTelemetry Collector with a single command.

FR-09.2: ECS task definitions and service configurations must deploy all consumer services to Fargate without manual configuration.

FR-09.3: Amazon MSK must be used as the Kafka cluster in the ECS and EKS deployments. Self-managed brokers are for local development only.

FR-09.4: Kubernetes manifests and a Helm chart must be provided for EKS deployment. The Helm chart must support separate values files for dev, staging, and production environments.

FR-09.5: All credentials must be stored in AWS Secrets Manager. No plaintext secrets in environment variables, task definitions, or Kubernetes manifests.

---

## Non-Functional Requirements

### NFR-01: Throughput

NFR-01.1: The platform must sustain 50,000 events per second end-to-end from producer to audit archive under sustained load for a minimum of 3 minutes.

NFR-01.2: The Trade Enrichment Service must process a single event within 10ms at p99 under nominal load of 10,000 events per second.

NFR-01.3: The Risk Alert Service must evaluate a single trade within 5ms at p99 under nominal load.

NFR-01.4: The total pipeline latency from producer publish to risk alert available on the notification topic must be below 200ms at p99 under nominal load.

### NFR-02: Reliability

NFR-02.1: No event must be permanently lost. Events that fail consumer processing must land in the DLQ. Events in the DLQ must be replayable.

NFR-02.2: The platform must survive the loss of one Kafka broker without message loss or consumer outage, with rebalancing completing within 30 seconds.

NFR-02.3: The platform must survive the loss of one consumer service instance without message loss, with KEDA replacing the instance within 60 seconds.

NFR-02.4: The Audit Service must achieve at-least-once delivery to S3. Idempotent consumers in downstream analytics systems must handle duplicates.

### NFR-03: Scalability

NFR-03.1: Consumer services must scale horizontally without code changes. Adding a new consumer instance must improve throughput linearly up to the number of topic partitions.

NFR-03.2: KEDA must demonstrate measurable scale-out within 90 seconds of a sustained consumer lag spike.

NFR-03.3: Kafka topics must be configured with a minimum of 12 partitions to allow scaling to 12 consumer instances per group.

NFR-03.4: The platform architecture must support adding new event types by adding new Avro schemas and consumer services without modifying existing services.

### NFR-04: Observability

NFR-04.1: Any event must be traceable end-to-end from producer to audit archive using a single correlationId query in Grafana Loki.

NFR-04.2: Consumer lag must be visible in Grafana within 15 seconds of the lag event occurring.

NFR-04.3: A service restart must be detectable in the pipeline health dashboard within 30 seconds.

NFR-04.4: The mean time to detect a pipeline failure must be under 2 minutes with the configured Prometheus alert rules.

### NFR-05: Security

NFR-05.1: All inter-service communication must use TLS in the ECS and EKS deployments.

NFR-05.2: All credentials must be retrieved from AWS Secrets Manager at service startup. No credentials in environment variables.

NFR-05.3: IAM roles must follow least privilege. Each ECS task must have a dedicated task role with only the permissions it requires.

NFR-05.4: S3 audit buckets must block all public access and enforce SSL-only access via bucket policy.

NFR-05.5: Kafka topics must have ACLs restricting producer and consumer access by service identity in the ECS and EKS deployments.

### NFR-06: Maintainability

NFR-06.1: Every architecture decision with more than one viable alternative must be documented in an ADR under `docs/adr/`.

NFR-06.2: A runbook must be maintained covering the top 10 operational scenarios: consumer lag spike, DLQ depth alarm, broker loss, schema deployment, consumer restart, replay from DLQ, Grafana alert investigation, KEDA scaling verification, MSK broker replacement, schema rollback.

NFR-06.3: CI must run on every pull request and must include: compilation, unit tests, integration tests with embedded Kafka, schema compatibility check, and Docker image build.

NFR-06.4: All Grafana dashboards must be version-controlled as JSON. No dashboard changes without a corresponding pull request.

### NFR-07: Cost

NFR-07.1: The local development environment must run entirely on Docker Compose with no cloud dependencies.

NFR-07.2: The ECS Fargate deployment must use Fargate Spot for non-critical services to reduce compute cost.

NFR-07.3: The EKS validation deployment must be designed to run for a single weekend and be torn down. Estimated cost must not exceed $15 USD for the validation run.

NFR-07.4: AWS Budgets must be configured with an alert at $10 USD and a hard stop at $20 USD for the EKS validation account.

---

## Constraints

The platform must be implemented in Java 25 with Spring Boot for all producer and consumer services.

Infrastructure as code must use AWS CDK for ECS deployment and Helm for EKS deployment.

Kafka client library must be the official Apache Kafka client wrapped by Spring Kafka.

Schema serialisation must use Avro. JSON serialisation is not permitted for event payloads.

All Docker images must be based on Amazon Corretto 25 to match the Lambda runtime used in the companion financial intelligence platform project.

---

## Assumptions

The simulator-generated market data is sufficient for load testing purposes. Licensed real-time feeds from NSE, BSE, or commercial providers are not required.

The EKS validation run is a one-time activity. Ongoing EKS costs are not budgeted.

KEDA is deployed as a cluster-level component and is available before consumer services are deployed.

MSK is used as the Kafka cluster in all cloud deployments. Self-managed Kafka brokers are restricted to local Docker Compose.
