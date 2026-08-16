# Financial Event Streaming Platform — Architecture (v2)

## Services

- **market-data-simulator** — pure Kafka producer, no HTTP surface. Two
  modes: realistic (GBM-based tick generation across configurable
  tickers, Pareto-distributed volume) and load-test (max sustained
  throughput, duration-configurable). This is the system's data source
  and its load-test harness in one service.
- **enrichment-service** — consumer group, KEDA-scaled on lag. Applies
  enrichment/derived-field logic to incoming events. This is the service
  whose scaling behavior is the centerpiece of the load-test story.
- **schema-registry** — Confluent-compatible, Avro schemas for every
  event type, compatibility checks (`BACKWARD` at minimum) enforced as a
  CI gate before any schema change reaches a topic.

## Why these architectural decisions, explicitly

### Exactly-once vs. at-least-once
Default to at-least-once with idempotent producers
(`enable.idempotence=true`) plus consumer-side deduplication on event ID,
rather than full Kafka transactions. Reasoning to document and be ready
to defend: transactional exactly-once adds real latency and complexity;
idempotent producer + dedup gets equivalent correctness for this
pipeline's shape (append-only enrichment, no multi-topic atomic writes)
at lower cost. This is exactly the kind of decision the research flags as
"architectural, not operational" — worth an ADR, not just a config flag.

### Durability and disaster recovery
Production config: `replication.factor=3`, `min.insync.replicas=2`,
brokers spread across availability zones. The part most deployments skip
(and the research specifically calls out as the differentiator): actually
kill a broker during the load test and confirm the pipeline keeps
serving, then document the observed behavior — not just the intended
behavior. A recovery path that's only ever been designed, never tested,
is a documented gap, not a documented capability.

### A migration-shaped producer mode (new in this version)
Add a third mode to `market-data-simulator`, or a small sibling service:
a **CDC-style backfill/migration producer** that simulates the strangler-
pattern scenario directly — reading from a mock "legacy" table (a plain
Postgres table standing in for a legacy system) via a Debezium-style
change stream and publishing equivalent events to Kafka, alongside the
live GBM stream. This doesn't change the core pipeline, but it makes the
single highest-demand consulting story (legacy batch system to
event-driven, without a rewrite) something you built and can show, not
just something you can describe.

## Deployment topology
Local (Docker Compose) for development. ECS Fargate + Amazon MSK for
production validation — this is where the load test actually runs and
where the recovery-path test happens. EKS manifests written and validated
once against the same workload, then torn down; documented as "here's the
Kubernetes path if you need it" rather than run continuously, which keeps
this a cost-conscious portfolio project rather than an ongoing expense.

## Observability
OpenTelemetry trace per event, correlation ID from producer through to
enrichment. Prometheus metrics on consumer lag, throughput, and error
rate. Grafana dashboards built specifically to produce the screenshot
sequence that tells the load-test story: calm pipeline, growing load,
spike, KEDA scaling response, recovery, drain back to baseline. That
sequence of screenshots is the artifact — both for an interview panel and
for a consulting pitch deck.

## Load test design (k6)
Five phases against `market-data-simulator` in load-test mode:
1. Calm baseline
2. Ramping load
3. Spike to 50,000 events/sec
4. Sustained peak — measure p99 processing latency here, this is the
   number that goes in the README
5. Drain to zero — watch lag drain and KEDA scale back down to minimum

## Failure handling
Dead letter topics per consumer group, retry with backoff, circuit
breaker on downstream dependencies, poison-pill detection (a
malformed/unparseable event goes to DLQ immediately rather than blocking
the partition).

## What's deliberately out of scope for v1
Full Kafka transactions/exactly-once semantics (documented as a
considered-and-rejected option, not an oversight). Multi-region active-
active replication (single-region, multi-AZ is the documented v1
boundary). Flink — Kafka Streams-level processing is sufficient for this
pipeline's shape; noted as a deliberate scope boundary given Flink's
rising adoption for heavier stateful joins, not a gap.
