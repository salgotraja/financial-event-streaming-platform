# Architecture

Financial Event Streaming Platform

Version 1.2 Status: Draft — Security-First + Agentic Investigation Revision

---

## Version 1.2 Change Summary

Version 1.2 preserves the full v1.1 security/control architecture and adds three new planes without placing them in the deterministic hot path: a CDC migration plane, a reconciliation-observation/candidate plane, and an AI-assisted investigation/evaluation plane. The revision also normalizes the supplied golden dataset against explicit event contracts and makes AI permissions, observability, cost and regression evidence first-class architecture concerns.

---

## Overview

The Financial Event Streaming Platform is a security-first Kafka-based event streaming and financial-risk control system. It ingests trade executions, market-data ticks, corporate actions, and instrument reference data; enriches trades; evaluates real-time risk; maintains position/exposure read models; governs risk-rule changes; creates investigation cases; and archives events to an immutable, tamper-evident S3 audit trail.

The architecture follows five principles. First, every component is independently scalable. Second, observability is designed into every service from the first commit. Third, failure is expected and recoverable through retries, DLQs, replay controls, and reconciliation. Fourth, **identity is a platform primitive**: humans and workloads authenticate explicitly and receive narrowly scoped authorization. Fifth, sensitive financial/operational actions are governed and produce durable evidence.

---


## Architectural Planes

The platform is intentionally split into five planes with different reliability, latency, identity, and cost characteristics.

```text
                   ┌───────────────────────────────┐
                   │  1. INGESTION / MIGRATION    │
                   │ live simulators + Debezium   │
                   └──────────────┬────────────────┘
                                  │
                                  ▼
                   ┌───────────────────────────────┐
                   │ 2. DETERMINISTIC STREAMING   │
                   │ enrich / risk / positions    │
                   │ 50k events/sec target        │
                   └──────────────┬────────────────┘
                                  │
                 ┌────────────────┴──────────────────┐
                 ▼                                   ▼
     ┌────────────────────────┐          ┌────────────────────────┐
     │ 3. AUDIT / CONTROL     │          │ 4. CANDIDATE / CASE   │
     │ immutable evidence     │          │ deterministic screen   │
     └────────────────────────┘          └────────────┬───────────┘
                                                     │ bounded queue
                                                     ▼
                                          ┌────────────────────────┐
                                          │ 5. AGENT INVESTIGATION │
                                          │ tools / graph / eval   │
                                          │ human review required  │
                                          └────────────────────────┘
```

**Critical design invariant:** Plane 5 is asynchronous and optional. Loss, throttling, provider outage, graph outage, or budget exhaustion in the agentic plane cannot block Planes 1–3 or increase the deterministic risk pipeline's availability dependency graph.

The same event may therefore have two different service-level objectives: sub-200ms deterministic risk evaluation and a seconds/minutes investigation SLA for a bounded anomaly candidate.

---

## System Context

```
External / Synthetic               Event & Risk Platform                    State / Evidence
--------------------               ---------------------                    ----------------

Market Data Simulator  ------>  Market Data Producer  ------>  Kafka/MSK
Trade Simulator        ------>  Trade Producer        ------>  Kafka/MSK
Corporate Actions      ------>  Corporate Action      ------>  Kafka/MSK
Instrument Master      ------>  Reference Data        ------>  Kafka/MSK

                                  Trade Enrichment      <----  Kafka/MSK
                                      |                  <-->  Redis
                                      |                  <-->  instrument cache
                                      v
                                  trades.enriched
                                      |
                       +--------------+---------------+
                       |                              |
                       v                              v
                Risk Alert Service           Position & Exposure
                       |                      Read Model Service
                       |                              |
                       v                              v
             notifications.alerts              PostgreSQL
                       |
                       v
                Alert Case Service  ------>  Case Store

Risk Maker/Checker ---> Admin Control Plane ---> Risk Rule Governance
Operators/Auditors ---> OIDC + Policy PDP       DLQ Replay / Case Actions
                                      |
                                      v
                               risk-rules.events

All event/control topics ----------> Audit Service ----------> S3 Object Lock
                                      |                         + signed manifests
                                      v
                              Reconciliation Service
                                      |
                                      v
                              controls.reconciliation

All workloads -------------> Workload Identity / IAM / mTLS
All privileged actions ----> security.events / CloudTrail / Security Dashboard

Operators <----------------- Grafana / Prometheus / Loki / Jaeger/X-Ray
```

---

## Component Architecture

### Kafka Cluster

Three brokers in local Docker Compose. Amazon MSK with three brokers across three availability zones in the cloud reference deployment. Production-style producers use `acks=all`, idempotence, replication factor 3, and `min.insync.replicas=2`.

Cloud authentication and authorization use **MSK IAM**. Every producer/consumer service has a dedicated AWS workload identity and an IAM policy restricted to required cluster/topic/group actions. Kafka ACLs are used only for local/non-IAM security profiles.

Topic design preserves the v1.0 pipeline and adds financial-control and security topics:

| Topic | Purpose | Retention | Partitions |
| --- | --- | ---: | ---: |
| `trades.raw` | Raw trade execution events | 7 days | 12 |
| `trades.enriched` | Enriched trade events | 7 days | 12 |
| `market-data.ticks` | Price tick stream | 1 day | 12 |
| `corporate-actions` | Corporate actions | 30 days | 6 |
| `reference-data.instruments` | Compacted instrument master | compacted | 6 |
| `positions.snapshots` | Position/exposure snapshots | 7 days | 12 |
| `notifications.alerts` | Risk alerts | 3 days | 6 |
| `risk-rules.events` | Versioned risk-rule lifecycle | 365 days | 6 |
| `alert-cases.events` | Investigation case lifecycle | 365 days | 6 |
| `controls.reconciliation` | Control/reconciliation results | 365 days | 6 |
| `security.events` | Application security and privileged-action events | 365 days | 6 |
| `{source-topic}.dlq` | Failed processing events | 30 days | source-aligned |

The 12-partition design remains the primary scaling ceiling for trade-path consumer groups without repartitioning. Control-plane topics intentionally use fewer partitions because ordering and auditability are more important than extreme throughput.


#### v1.2 Additional Topics

| Topic | Purpose | Suggested retention |
| --- | --- | --- |
| `legacy.trades.cdc` | Debezium CDC envelope from mock legacy source | 3 days |
| `reconciliation.observations` | Synthetic internal/external reconciliation observations | 7 days |
| `anomaly.candidates` | Bounded deterministic candidates for agent investigation | 3 days |
| `agent.decisions` | Structured agent decisions/evidence references | 30 days |
| `review.decisions` | Human reviewer verdicts and feedback events | 1 year |
| `remediation.requested` | Synthetic human-approved remediation intent only | 30 days |
| `precedent.graph.sync` | Idempotent graph projection updates | 7 days |

Topic retention is intentionally shorter for recomputable/derived candidate data and longer for human/control decisions. Immutable audit storage remains the authoritative long-term evidence path.


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

The target is 50,000 events per second, but the repository separates **production acceptance** from a synthetic throughput-ceiling experiment.

Production acceptance profile:

```text
batch.size=65536
linger.ms=5
compression.type=lz4
acks=all
enable.idempotence=true
buffer.memory=67108864
```

This profile is used for the headline throughput/latency result and broker-failure validation. A separate `acks=1` synthetic ceiling run is allowed only as a secondary experiment and is labelled non-durable. If the low-cost validation environment cannot reach 50,000 events/sec with production durability, the measured sustainable ceiling and bottleneck are published rather than hiding the trade-off.

### Trade Enrichment Service

Consumes from `trades.raw`. For each event: retrieves current market data from Redis cache, computes derived fields, publishes enriched event to `trades.enriched`.

Market-data cache population is event-driven: a cache projector consumes `market-data.ticks` and maintains the latest market state in Redis. The Trade Enrichment Service does not call the simulator synchronously.

On a cache miss, enrichment follows a bounded policy: retry/read a permitted recent value only when freshness policy allows it; otherwise emit an explicit `REFERENCE_DATA_UNAVAILABLE` failure to the retry/DLQ path. Cache misses, stale-value use, and data age are metrics.

Retry policy: 3 attempts with exponential backoff starting at 100ms. After the third failure, the event is published to `trades.raw.dlq` with full failure context.

Poison-pill handling is **per record**, not per event type. A malformed or permanently invalid record is quarantined after bounded retry and its offset is advanced so one bad event cannot block the partition. Circuit breakers are reserved for failing downstream dependencies (for example Redis/PostgreSQL), where opening the circuit protects the service from a cascading dependency failure.

Consumer group: `trade-enrichment-service`. One consumer group, 12 partitions, scales from 1 to 12 effective consumers via KEDA on EKS or ECS Service Auto Scaling on ECS.

### Risk Alert Service

Consumes from `trades.enriched`. Evaluates each trade against a configurable rule engine. Rules are loaded from a YAML configuration file and refreshed at runtime via Spring Cloud Config or a ConfigMap in Kubernetes.

Rule evaluation is stateful: position tracking requires a running total per trader per ticker. State is maintained in PostgreSQL with optimistic locking. The service uses a consistent hash of the ticker symbol to route events to the same partition, ensuring the same consumer instance handles all trades for a given ticker and avoids cross-instance state coordination.

Alert routing: CRITICAL alerts are published to `notifications.alerts` with immediate flush. WARNING and INFO alerts are batched and published every 5 seconds.

Consumer group: `risk-alert-service`.

### Audit Service

Consumes from all financial, control, and security topics using a dedicated consumer group. It buffers events in bounded batches and writes Parquet objects to S3 using date/event-type partitioning.

The Audit Service preserves at-least-once delivery. Topic/partition/offset remains the canonical event idempotency key for downstream analytics and reconciliation.

**v1.1 integrity model:** every audit flush produces a sidecar manifest containing the Parquet object SHA-256 digest, event count, topic/partition offset ranges, schema versions, Audit Service identity, and timestamps. In AWS, the manifest digest is signed with a dedicated asymmetric KMS `SIGN_VERIFY` key. Audit data and manifests are stored in an S3 bucket with versioning, Block Public Access, SSL-only policy, and Object Lock retention.

The Audit Service task/pod role can write objects and request `kms:Sign` for the audit-signing key but cannot delete objects, bypass retention, change bucket policy, or administer the key.

### Notification Service

Consumes `notifications.alerts`. It provides downstream dispatch and a read-only inspection endpoint. Privileged DLQ replay is removed from the Notification Service in v1.1 and moved to the authenticated Administrative Control Plane.

### Instrument Reference Data Service

Produces and maintains synthetic instrument metadata on the compacted `reference-data.instruments` topic. The enrichment service consumes this data into a local/Redis-backed cache so market capitalisation and instrument attributes are explicit data dependencies rather than hard-coded constants.

### Position & Exposure Read Model Service

Consumes `trades.enriched` and builds an idempotent CQRS-style read model by account, trader, and ticker. It publishes `positions.snapshots`, exposes read-only authorised queries, and can rebuild state from the event history. It is not an order book or settlement ledger.

### Risk Rule Governance Service

Owns rule versions and lifecycle states. Risk makers propose changes; independent risk checkers approve material changes. Approved changes publish to `risk-rules.events`; the Risk Alert Service updates its in-memory active rule set from this stream. Historical rules are immutable and rollback creates a new lifecycle event rather than rewriting history.

### Alert Case Service

Consumes WARNING/CRITICAL alerts and creates investigation cases. Case transitions are authenticated, authorised, reason-coded, and emitted to `alert-cases.events`. Case timelines provide a compact business demonstration of how event processing, human identity, policy decisions, and audit evidence intersect.

### Control & Reconciliation Service

Compares event counts and topic/partition offset ranges with signed audit manifests. It detects gaps, unexpected duplicates, and incomplete archive coverage and publishes results to `controls.reconciliation`. A failed control is both a business/control signal and a security/reliability incident.

---


### Legacy CDC / Migration Plane

A mock `legacy-source-postgres` database represents an existing batch-era trading source. A Debezium PostgreSQL connector performs an initial snapshot and then streams row-level changes into `legacy.trades.cdc`.

A `migration-normalizer-service` converts CDC envelopes to the canonical `TradeEvent` contract. It adds source-system provenance and an idempotency key, then publishes into `trades.raw`. This creates a real strangler-pattern coexistence path: live synthetic events and migrated legacy events share the same canonical downstream pipeline.

PostgreSQL source data remains synthetic. The migration plane exists to demonstrate schema mapping, dual-run validation, restart/cutover, deduplication, and backfill/CDC reconciliation rather than to emulate a specific bank's legacy system.

### Reconciliation Observation Simulator

`reconciliation-simulator` generates synthetic internal/external observations that make the settlement/reconciliation golden cases architecturally real without turning the system into a payment or settlement engine.

It emits `reconciliation.observations` events for:

- duplicate/reference recurrence;
- internal-vs-external amount mismatch;
- mutually exclusive or out-of-order state transitions;
- missing multi-party legs/instructions;
- benign timing, fee, corporate-action, or rounding explanations.

The event contract records source provenance and completeness state so the platform can distinguish "known mismatch" from "possibly still in flight."

### Deterministic Anomaly Candidate Service

The agent does **not** consume the full `trades.enriched` stream. `anomaly-candidate-service` performs cheap deterministic screening across enriched trades, risk alerts, reconciliation observations, corporate-action/reference context, and selected position/exposure signals.

It publishes a bounded `AnomalyCandidateEvent` to `anomaly.candidates`, including the deterministic trigger/evidence that promoted the item.

This service is independently scalable and may enforce rate limits, priority classes and normal-case sampling. Candidate backlog never backpressures the core streaming/risk plane.

### Agent Investigation Service

`agent-investigation-service` consumes `anomaly.candidates` and produces structured investigation decisions.

Allowed capabilities are intentionally narrow:

```text
READ-ONLY                           MUTATING
---------                           --------
ledger-lookup                       flag-for-review
reference/calendar lookup           (proposal only)
position/history lookup
precedent-graph retrieval
```

The agent has no credential for risk-rule approval, DLQ replay, ledger mutation, audit deletion, or remediation execution.

A decision is a typed object rather than free-form prose:

```text
outcome: FLAG | NO_FLAG | ESCALATE | INCONCLUSIVE
severity: INFO | WARNING | CRITICAL
confidence: LOW | MEDIUM | HIGH | NOT_APPLICABLE
reasonCodes: [...]
evidenceRefs: [...]
toolStatus: [...]
precedentRefs: [...]
narrative: "..."
```

The narrative is a presentation artifact; authorization and workflow are driven by structured fields.

### Critique / Reflection Control

A second critique pass is bounded to one revision. It is triggered by:

- medium/low confidence;
- required tool/evidence failure;
- high-impact candidate category;
- policy-defined uncertainty or contradiction; or
- a small configured sample of high-confidence results for calibration surveillance.

The second pass receives the draft and evidence with a separate critic instruction. It is not described as mathematically independent if it uses the same underlying model.

### Human Review Service

Agent flags/escalations create a proposed `ReviewCase`. Authenticated human reviewers can approve, reject, request more evidence, or close/escalate a case under the existing control-plane authorization model.

For v1.2, "approved remediation" means only a case-state transition or synthetic `remediation.requested` event. No real ledger, account, order or settlement state is modified.

### Precedent Graph

Neo4j stores a **derived** case-history graph. PostgreSQL remains authoritative for cases and reviewer verdicts.

Suggested graph:

```text
(ReviewCase)-[:ABOUT]->(Event)
(Event)-[:INSTRUMENT]->(Instrument)
(Event)-[:COUNTERPARTY]->(Counterparty)
(ReviewCase)-[:MATCHES_PATTERN]->(AnomalyPattern)
(ReviewCase)-[:RESOLVED_AS]->(Resolution)
(ReviewCase)-[:SIMILAR_TO]->(ReviewCase)
```

Human-review events update the graph asynchronously through an idempotent sync consumer. The graph can be rebuilt from authoritative case history; therefore a Neo4j outage degrades precedent retrieval but does not block deterministic processing.

### Evaluation & Regression Plane

The supplied 15-case golden dataset is treated as a **versioned regression suite**, not a population-level accuracy benchmark.

Evaluation has three layers:

1. deterministic assertions for outcome, severity, required tool behavior, mandatory evidence and forbidden behavior;
2. calibrated rubric/judge scoring only for narrative/evidence quality that cannot be checked deterministically; and
3. adversarial tests for prompt injection, excessive agency, missing evidence, fabricated tool completion and approval bypass.

False negatives, false positives, tool-failure/escalation correctness, and confidence calibration are reported separately.

### AI Audit and Observability

The existing trace is extended across the asynchronous candidate/agent boundary. When a strict parent-child relationship is misleading because of queue delay or fan-out, spans use trace links while preserving the originating event/candidate identifiers.

Telemetry records decision metadata, model/provider, prompt/tool versions, token counts, latency, tool status, critique trigger and estimated cost. Raw prompts, retrieved confidential payloads, credentials and hidden chain-of-thought are not required for observability.


---

## Identity and Security Architecture

Security is not a perimeter layer. Every request or event-producing workload is associated with an identity, a policy boundary, and observable evidence.

### Identity Populations

**Human identities:** `PlatformAdmin`, `RiskMaker`, `RiskChecker`, `Operator`, `ComplianceAuditor`, `SecurityAuditor`. Humans authenticate through an OIDC-compatible IdP. Keycloak is the local/reference IdP; cloud deployments can federate an enterprise IdP without changing application authorization semantics.

**Workload identities:** every service receives its own identity. ECS uses task roles; EKS uses a dedicated Kubernetes service account associated with EKS Pod Identity. No application service uses a long-lived AWS access key.

**Service identity:** the EKS advanced-security profile adds SPIFFE/SPIRE to attest selected workloads and issue short-lived SVIDs for service-to-service mTLS. SPIFFE identity complements AWS IAM; it does not replace AWS IAM authorization to AWS resources.

### Authorization Planes

```
Human ----OIDC----> Admin API / PEP ----> Policy PDP ----> privileged operation
                                         |  role
                                         |  action
                                         |  resource
                                         |  reason/context
                                         v
                                      allow/deny

Workload ----temporary AWS identity----> IAM ----> MSK / S3 / KMS / RDS / Cache

Workload ----SPIFFE SVID (EKS advanced profile)----> mTLS peer workload
```

Administrative operations are denied by default. Maker-checker is enforced at the policy/business layer, not merely by UI convention.

### Workload Least Privilege

| Workload | Kafka read | Kafka write | AWS/data access |
| --- | --- | --- | --- |
| Trade Producer | — | `trades.raw` | MSK connect/write only |
| Market Data Producer | — | `market-data.ticks` | MSK connect/write only |
| Reference Data | — | `reference-data.instruments` | MSK connect/write only |
| Enrichment | `trades.raw`, reference data | `trades.enriched`, own DLQ | Redis read/write as scoped |
| Risk Alert | `trades.enriched`, `risk-rules.events` | `notifications.alerts`, own DLQ | risk-state DB only |
| Position Service | `trades.enriched` | `positions.snapshots` | position DB only |
| Alert Case | `notifications.alerts` | `alert-cases.events` | case DB only |
| Audit | all approved platform topics | — | S3 PutObject + KMS Sign only |
| Reconciliation | required control/audit metadata | `controls.reconciliation` | read-only audit metadata + KMS Verify |

Negative tests verify that each workload is denied from resources belonging to other workloads.

### Kafka/MSK Security

Cloud MSK uses TLS plus IAM authentication/authorization. IAM policies scope `Connect`, topic `ReadData`/`WriteData`, consumer group access, and transactional permissions by service identity. Private networking is required; brokers are not internet-facing.

Local development supports two profiles: `dev` for rapid iteration and `strict-security` for authenticated Kafka/TLS and negative-authorization tests.

### Secrets and Credentials

The hierarchy is:

1. workload identity / temporary credentials;
2. short-lived generated credentials (for example IAM database authentication where suitable);
3. rotated Secrets Manager secrets only when a target system genuinely requires a shared secret/password.

Endpoints and bootstrap addresses are configuration, not secrets, and are not stored in Secrets Manager merely to hide them.

### Data Protection and Cryptographic Integrity

MSK, S3, databases, and registries use encryption at rest. The audit archive uses a customer-managed KMS key and Object Lock. Audit manifests are cryptographically signed to detect tampering. Secret/key administration is separated from application execution roles.

### Network Security

Data-plane components run in private networking. Security groups are service-specific. Public ingress to MSK, PostgreSQL/RDS, Redis/ElastiCache, Schema Registry, telemetry backends, and admin services is prohibited. AWS service access uses VPC endpoints where practical during cloud validation.

### Human Privileged Operations

The Administrative Control Plane owns DLQ replay, rule governance, case disposition, audit export, and privileged diagnostics. Every operation records actor, role, reason, target, policy decision, request/correlation IDs, and outcome. Sensitive operations are included in `security.events` and the immutable audit path.

### Security Telemetry

The platform emits security-specific metrics/events for failed authentication, denied authorization, privileged actions, unexpected workload access, secret/key failures, integrity verification, and reconciliation failures. CloudTrail augments application telemetry for AWS control-plane evidence.

### Software Supply Chain

CI adds secret scanning, SAST, dependency/SCA scanning, IaC/policy validation, SBOM generation, image scanning, and image signing. Production/security-validation deployment uses immutable image digests; the EKS advanced profile validates image signatures before deployment.

---

## Observability Architecture

Observability is not a layer added after the services are built. It is designed into each service from the first commit.

### OpenTelemetry

The OpenTelemetry Java agent is attached to every service via the `JAVA_TOOL_OPTIONS` environment variable. Auto-instrumentation covers Spring Boot, Kafka client, JDBC, and Redis.

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

KEDA reads `kafka_consumer_lag_by_partition` directly from Prometheus to make scaling decisions. The metric must be available and accurate for autoscaling to function.


Security metrics added in v1.1:

```
# Authorization
security_authorization_decisions_total{subject_type,service,action,decision}
security_authentication_failures_total{source,reason}
privileged_actions_total{action,role,outcome}

# Workload identity
workload_credential_failures_total{service,provider,reason}
msk_authorization_denied_total{service,topic,operation}

# Audit integrity
archive_manifest_sign_total{status}
archive_manifest_verify_total{status}
reconciliation_controls_total{control,status}

# Governance
risk_rule_changes_total{state,action}
maker_checker_denials_total{reason}
```


### Grafana

Four dashboards committed as JSON under `observability/grafana/dashboards/`.

Pipeline health dashboard: consumer lag per partition (heatmap), throughput per topic (time series), error rate per service (gauge), DLQ depth per topic (stat), and active replica/task count per service (time series). This is the dashboard open during deployments and incidents.

Service latency dashboard: p50, p95, p99 processing time per event type per service (time series), latency histogram distribution at current moment (heatmap), top 10 slowest event types (table), cache miss rate (gauge). This is the dashboard used during performance investigations.

Security & identity dashboard: authentication failures, authorization denials, privileged actions, workload credential errors, MSK denied access, risk-rule approvals, audit signature verification, and reconciliation failures.

Business signals dashboard: trades processed per minute (time series), risk alerts by severity over time (stacked bar), corporate actions processed (counter), audit lag (the time between event production and S3 availability), DLQ events by failure reason (pie chart). This is the dashboard a business stakeholder reads.

Alert rules in `observability/prometheus/alerts.yml`:

```
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

Every log line includes: `timestamp`, `level`, `service`, `correlationId`, `traceId`, `spanId`, `topic`, `partition`, `offset`, `processingLatencyMs`, `eventType`, `environment`.

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

Autoscaling differs by compute platform.

### EKS

Kubernetes manifests are wrapped in a Helm chart with separate environment values. Each application Deployment uses a dedicated Kubernetes service account; cloud access is provided through EKS Pod Identity. Pods run with restricted security contexts, non-root users, dropped capabilities, and read-only root filesystems where feasible.

KEDA is installed as an add-on and scales consumers from lag. NetworkPolicies restrict east-west traffic. The advanced-security profile deploys SPIRE and assigns SPIFFE IDs to at least the Enrichment, Risk, and Audit workloads for service-to-service mTLS experiments.

The EKS run remains an ephemeral validation environment. Before launch, IaC produces a cost estimate and a tagged teardown plan. Budgets/alerts and deny-new-provisioning guardrails complement, but do not replace, deterministic teardown automation.

---

## Data Flow

### Trade-to-Risk-to-Audit Lifecycle

1. Trade Producer generates a TradeEvent with correlation ID and trace context and authenticates to MSK using its workload identity.
2. MSK IAM authorizes that identity to write only `trades.raw`.
3. Trade Enrichment consumes the event, obtains instrument/market data, enriches it, and writes `trades.enriched` using its own workload identity.
4. Risk Alert Service evaluates the event against the currently approved rule version and emits a RiskAlertEvent when required.
5. Position & Exposure Service updates its idempotent read model and emits snapshots.
6. Alert Case Service creates a case for WARNING/CRITICAL alerts.
7. Audit Service independently consumes approved topics, writes Parquet, creates a cryptographic manifest, signs it in AWS, and stores both under Object Lock.
8. Reconciliation validates event/offset coverage against audit manifests and emits control evidence.
9. OpenTelemetry carries trace context end-to-end, while security events record identity and authorization decisions without credentials.

The original performance objective remains: producer publish to risk-alert availability below 200 ms p99 under nominal 10,000 events/second load. Security controls on the hot path must be benchmarked so their latency contribution is explicit.

### Risk-Rule Change Lifecycle

```
RiskMaker -> OIDC -> Admin Control Plane -> PDP -> create DRAFT/PENDING_APPROVAL
                                                       |
RiskChecker -> OIDC -> Admin Control Plane -> PDP ------+
                                                       v
                                               risk-rules.events
                                                       |
                                                       v
                                                Risk Alert Service
                                                       |
                                                       v
                                                 security.events
```

The same actor cannot create and approve a material rule version.

### Privileged DLQ Replay Lifecycle

An Operator authenticates, supplies a ticket/reason and target range, receives an authorization decision, and invokes replay through the control plane. The action, policy result, event range, and outcome are emitted to `security.events` and archived. The Notification Service cannot replay DLQ messages directly.

---


### Legacy Migration Lifecycle

```text
Legacy PostgreSQL
    │ snapshot + WAL changes
    ▼
Debezium
    │
    ▼
legacy.trades.cdc
    │
    ▼
Migration Normalizer
    │ canonical mapping + idempotency + provenance
    ▼
trades.raw
    │
    └──► existing enrichment/risk/audit pipeline
```

Validation compares source rows/changes with canonical event counts, incompatible-record totals and deduplicated outputs before cutover.

### Candidate-to-Human-Review Lifecycle

```text
Enriched/Risk/Reconciliation signals
              │
              ▼
 Deterministic Candidate Service
              │
              ▼
      anomaly.candidates
              │
              ▼
    Agent Investigation
       │      │       │
       │      │       └──► Neo4j precedent (derived)
       │      └──────────► read-only tools
       ▼
 structured draft decision
              │
       bounded critique?
              │
              ▼
       final decision
              │
      flag / escalate only
              ▼
        Review Queue
              │
              ▼
     Authenticated Human
        approve/reject
              │
              ▼
    review.decisions + audit
              │
              └──► precedent graph sync / eval corpus
```

### Agent Failure Lifecycle

Provider timeout, graph outage, required tool failure, budget exhaustion, or iteration-limit exhaustion produces an explicit `ESCALATE`/degraded result. It does not silently become `NO_FLAG`, and it does not pause deterministic streaming.


---

## Architecture Decision Records

The following decisions are documented under `docs/adr/`:

- ADR-001: MSK over self-managed Kafka for cloud deployments
- ADR-002: Avro over JSON for financial event serialisation
- ADR-003: KEDA for EKS Kafka consumer autoscaling
- ADR-004: ECS Fargate as the primary AWS container validation path
- ADR-005: Parquet for audit storage
- ADR-006: GBM/Pareto synthetic market model
- ADR-007: Redis for low-latency market/reference cache
- ADR-008: PostgreSQL for risk/position/case state
- ADR-009: MSK IAM authentication and authorization in AWS
- ADR-010: Per-service ECS task roles and EKS Pod Identity
- ADR-011: Human OIDC identity + externalized policy decision for privileged APIs
- ADR-012: S3 Object Lock + signed audit manifests
- ADR-013: Secrets as fallback; workload identity as default
- ADR-014: SPIFFE/SPIRE advanced profile for service identity
- ADR-015: ECS Application Auto Scaling instead of KEDA on ECS
- ADR-016: Risk-rule maker-checker governance
- ADR-017: Event-driven position/exposure read model
- ADR-018: Security telemetry as first-class observability
- ADR-019: At-least-once + idempotent producer/consumer deduplication over blanket Kafka transactions
- ADR-020: Debezium CDC strangler-pattern migration path
- ADR-021: Deterministic candidate screening before LLM investigation
- ADR-022: Neo4j derived precedent graph; PostgreSQL remains source of truth
- ADR-023: Agent tool isolation + human-gated mutation boundary
- ADR-024: Golden-dataset regression gate and adversarial evaluation
- ADR-025: Structured decision/evidence trace instead of raw chain-of-thought logging
- ADR-026: Deterministic and agentic performance/cost isolation
- ADR-027: Per-record poison quarantine + event-fed market-data cache

---

## Technology Stack

| LayerTechnologyVersionRationale |                        |      |                                           |
| ------------------------------- | ---------------------- | ---- | ----------------------------------------- |
| Language                        | Java                   | 25   | LTS, virtual threads, AOT caches          |
| Framework                       | Spring Boot            | 3.4  | Spring Kafka, Actuator, Cloud Config      |
| Messaging                       | Apache Kafka           | 3.7  | Industry standard, MSK compatible         |
| Schema                          | Avro + Schema Registry | 7.6  | Backward compatibility enforcement        |
| Cache                           | Redis                  | 7.2  | Sub-millisecond market data lookup        |
| Database                        | PostgreSQL             | 16   | Risk position state with ACID guarantees  |
| Storage                         | S3 + Parquet           | -    | Cost-efficient audit archive              |
| Tracing                         | OpenTelemetry          | 2.x  | Vendor-neutral, auto-instrumentation      |
| Metrics                         | Prometheus             | 2.x  | KEDA integration, Grafana data source     |
| Logging                         | Loki                   | 3.x  | Log aggregation with label filtering      |
| Dashboards                      | Grafana                | 11.x | Dashboard-as-code JSON                    |
| Autoscaling (EKS)               | KEDA                   | 2.x  | Kubernetes Kafka lag-based scaling        |
| Autoscaling (ECS)               | Application Auto Scaling | -  | CloudWatch lag-derived ECS task scaling   |
| IaC (ECS)                       | AWS CDK                | 2.x  | Java-native infrastructure                |
| IaC (EKS)                       | Helm                   | 3.x  | Kubernetes packaging standard             |
| Human identity (reference)      | Keycloak               | current | OIDC for privileged control plane       |
| Policy decision                 | OPA / Rego             | current | Externalized privileged authorization  |
| Workload identity (AWS)         | ECS Task Role / EKS Pod Identity | - | Temporary AWS credentials      |
| Workload identity (advanced)    | SPIFFE / SPIRE         | current | Attested service identity / mTLS       |
| Key management                  | AWS KMS                | -    | Encryption and audit-manifest signing     |
| CI                              | GitHub Actions         | -    | Build, test, schema + security gates      |
| Kafka (cloud)                   | Amazon MSK             | -    | Managed brokers, no operational overhead  |
| Compute (cloud)                 | ECS Fargate            | -    | Serverless containers, no node management |
| CDC                             | Debezium PostgreSQL Connector | current | Real legacy-change capture / migration exercise |
| Precedent graph                 | Neo4j Community / rebuildable hosted validation | current | Graph traversal + GraphRAG learning; derived store |
| AI evaluation                  | Framework-agnostic JSON + adapter | - | Avoid hard-coupling CI evidence to one eval vendor |
| Agent runtime                  | Provider/model adapter behind typed tool gateway | - | Model portability + enforceable tool boundary |

---

## Security Validation Philosophy

A security feature is complete only when the repository demonstrates both an allowed path and a denied/failed path. Examples: Trade Producer can write `trades.raw` but cannot read it; Risk Service cannot write audit S3; Operator cannot approve a rule they created; Audit Service cannot delete locked audit objects; a workload with the wrong SPIFFE identity is rejected; and unsigned/untrusted container images fail the advanced deployment gate.
