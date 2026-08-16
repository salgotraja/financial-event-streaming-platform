# Requirements

Financial Event Streaming Platform

Version 1.2 Status: Draft — Security-First + Agentic Investigation Revision

---

## Version 1.2 Change Summary

Version 1.2 preserves every v1.1 capability and adds four deliberately bounded learning/portfolio extensions:

1. a real CDC/strangler-pattern migration path using a mock legacy PostgreSQL source and Debezium;
2. a synthetic reconciliation-observation stream so settlement/reconciliation anomaly scenarios have an explicit data contract rather than appearing only in AI tests;
3. an asynchronous agentic investigation plane that operates on deterministic anomaly candidates instead of sitting in the 50,000-events/second hot path; and
4. a versioned evaluation/regression harness with a 15-case golden dataset, adversarial cases, human-review feedback, and CI gates.

The revision also tightens AI safety: the agent receives least-privilege workload identity, untrusted event payloads are treated as data rather than instructions, consequential state changes remain human-gated, and audit records capture structured evidence/tool traces rather than hidden chain-of-thought.

---

## Purpose

This document defines the functional and non-functional requirements for a production-grade, security-first Kafka-based financial event streaming, risk-control, reconciliation-observation, and AI-assisted investigation platform. The platform ingests real-time financial events, processes them through enrichment and deterministic risk/anomaly pipelines, maintains operational position/exposure views, governs risk-rule changes, creates alert cases, archives events to an immutable and tamper-evident audit trail, and optionally uses a bounded LLM agent to investigate selected anomaly candidates and prepare evidence-backed case narratives for human review.

The project is intentionally designed to demonstrate Principal Architect-level capabilities across distributed streaming systems, financial-event processing, legacy-to-streaming migration, workload and human identity, authorization, observability, resiliency, autoscaling, cloud security, AI safety/evaluation, and operational governance on AWS.

---

## Scope

The platform covers the following boundaries.

**In scope:** event production, schema management, reference/instrument data, consumer pipeline services, trade enrichment, deterministic real-time risk evaluation, position/exposure read models, risk-rule governance, alert case lifecycle, immutable audit and reconciliation controls, synthetic internal-vs-external reconciliation observations, a mock legacy PostgreSQL source with CDC/backfill migration, deterministic anomaly-candidate generation, bounded AI-assisted case investigation, precedent retrieval, human review/feedback, evaluation/regression testing, dead-letter handling and controlled replay, human operator authentication/authorization, workload identity, service-to-service trust, cloud IAM, security telemetry, observability, KEDA-based EKS autoscaling, ECS Application Auto Scaling, ECS Fargate deployment, EKS manifests, infrastructure as code, software-supply-chain controls, and security/failure testing.

**Out of scope:** order management systems, order routing, trade execution against real exchanges, production trade settlement, money movement, automatic correction of financial ledgers, actual regulatory reporting to SEBI, regulatory certification, authenticated market-data feeds from licensed providers, autonomous agent remediation without human review, and storage of real customer/trader PII. All financial identifiers and reconciliation records are synthetic.

### Security-First Design Principles

1. **Identity before credentials.** Every human and workload receives an explicit identity; long-lived static credentials are avoided wherever temporary workload credentials are possible.
2. **Deny by default and least privilege.** Every service is restricted to the exact topics, consumer groups, data stores, keys, APIs, and administrative operations it requires.
3. **Authentication is not authorization.** Identity establishment, role/attribute evaluation, and business approval are modelled separately.
4. **Sensitive actions require evidence.** Risk-rule activation, privileged replay, case closure, policy changes, and emergency access produce durable audit evidence.
5. **Security must be observable.** Authentication failures, authorization denials, privilege use, policy changes, secret/key events, and anomalous access are measurable signals.
6. **Failure and compromise are assumed.** The design must limit blast radius when a service identity, operator session, secret, container, or node is compromised.
7. **No security-by-diagram.** Security requirements must have automated or repeatable validation tests, including negative permission tests.

---

## Stakeholders

| Stakeholder | Interest |
| --- | --- |
| Platform engineers | Build and operate the streaming platform and infrastructure |
| Risk analysts | Consume real-time alerts, positions, exposures, and risk evidence |
| Risk makers / checkers | Propose and independently approve material risk-rule changes |
| Compliance / audit | Query immutable records, control evidence, case history, and privileged actions |
| Operations / SRE | Monitor pipeline health, incidents, scaling, replay, and service availability |
| Security / IAM | Define identities, trust boundaries, least-privilege policies, and security telemetry |
| Application developers | Build producers/consumers under secure platform contracts |

---

## Functional Requirements

### FR-01: Event Production

The platform must produce three categories of financial events to Kafka topics.

FR-01.1: The Trade Producer must publish trade execution events to the `trades.raw` topic. Each event must include trade ID, ticker symbol, quantity, price, side (buy or sell), trader ID, timestamp, and correlation ID.

FR-01.2: The Market Data Producer must publish price tick events to the `market-data.ticks` topic at configurable rates. Each event must include ticker symbol, bid price, ask price, last traded price, volume, and timestamp.

FR-01.3: The Corporate Action Producer must publish corporate action events to the `corporate-actions` topic. Supported action types: dividend declaration, stock split, earnings announcement, rights issue.

FR-01.4: In load simulation mode, the Market Data Simulator must sustain a configurable production rate from 1,000 to 50,000 events per second using a Geometric Brownian Motion price model and Pareto-distributed volume.

FR-01.5: All producers must inject OpenTelemetry trace context into Kafka message headers to enable end-to-end distributed tracing.

### FR-02: Schema Management

FR-02.1: All events must be serialised using Apache Avro with schemas registered in Confluent Schema Registry.

FR-02.2: Schema evolution must be validated in CI. A schema change that breaks backward compatibility must fail the pipeline before merge.

FR-02.3: The platform must support at least two schema versions active simultaneously during rolling upgrades.

### FR-03: Trade Enrichment Service

FR-03.1: The service must consume from `trades.raw` and enrich each trade with current market data retrieved from Redis. Redis market state is populated asynchronously from `market-data.ticks`; the enrichment hot path must not synchronously call the market-data simulator.

FR-03.2: Enrichment must add: mid-price at execution time, spread at execution time, volume-weighted average price for the ticker over the last 5 minutes, and market capitalisation.

FR-03.3: Enriched events must be published to `trades.enriched`.

FR-03.4: If enrichment fails after 3 retry attempts with exponential backoff, the event must be published to `trades.raw.dlq`with the failure reason, original payload, retry count, and timestamp of first failure.

FR-03.5: Poison-pill handling must be per record. Permanently invalid events are quarantined after bounded retry and must not block the partition or cause an event-type-wide circuit breaker. Circuit breakers apply only to dependency failures.

### FR-04: Risk Alert Service

FR-04.1: The service must consume from `trades.enriched` and evaluate each trade against configurable risk rules.

FR-04.2: The following risk rules must be supported:

Position limit breach: alert when a single trader's net position in a ticker exceeds a configurable threshold.

Unusual volume: alert when trade volume exceeds 3 standard deviations above the rolling 60-minute mean for that ticker.

Price deviation: alert when execution price deviates more than 2 percent from the last market data tick.

Wash trade detection: alert when buy and sell of the same ticker by related accounts occur within 60 seconds.

FR-04.3: Risk alerts must be published to `notifications.alerts` with severity (INFO, WARNING, CRITICAL), alert type, triggering trade ID, and evaluated rule parameters.

FR-04.4: Rules must be configurable at runtime without service restart.

### FR-05: Audit Service

FR-05.1: The service must consume from all topics and write every event to S3 in Parquet format.

FR-05.2: S3 keys must be partitioned by year, month, day, and event type to support efficient Athena queries.

FR-05.3: Audit records must be immutable. No update or delete operations are permitted on audit data.

FR-05.4: The service must guarantee at-least-once delivery to S3. Duplicate events in the audit trail are acceptable. Missing events are not.

FR-05.5: Audit records must be queryable via Athena within 5 minutes of event production.

### FR-06: Dead Letter Queue Management

FR-06.1: Every primary topic must have a corresponding DLQ topic with the naming convention `{topic}.dlq`.

FR-06.2: DLQ events must include: original topic, original partition, original offset, failure reason, exception class, stack trace summary, retry count, first failure timestamp, last failure timestamp.

FR-06.3: The platform must expose an API endpoint to replay events from a DLQ back to the original topic after manual inspection.

FR-06.4: DLQ depth must be monitored and an alert must fire when any DLQ exceeds 100 unprocessed messages.

### FR-07: Observability

FR-07.1: All services must emit OpenTelemetry traces with spans for Kafka produce, Kafka consume, external cache access, and database access.

FR-07.2: All services must expose Prometheus metrics on a `/actuator/prometheus` endpoint.

FR-07.3: The following custom metrics must be present in every consumer service:

`kafka_consumer_lag_by_partition` with labels for topic, partition, and consumer group.

`event_processing_duration_seconds` histogram with labels for service and event type.

`events_processed_total` counter with labels for service, topic, and status (success, failure, dlq).

`dlq_events_total` counter with labels for topic and failure reason.

FR-07.4: Four Grafana dashboards must be committed to the repository as JSON: pipeline health, service latency, business/control signals, and security/identity.

FR-07.5: Alert rules must be defined in Prometheus for: consumer lag above 1,000 per partition for more than 60 seconds, error rate above 1 percent over 5 minutes, DLQ depth above 100, service instance count at zero.

FR-07.6: All log lines must be structured JSON with the following mandatory fields: timestamp, level, service, correlationId, traceId, spanId, topic, partition, offset, processingLatencyMs.

### FR-08: Autoscaling

FR-08.1: EKS deployments must use KEDA to scale Kafka consumer Deployments from consumer lag. ECS deployments must use Amazon ECS Service Auto Scaling / Application Auto Scaling with a lag-derived CloudWatch metric.

FR-08.2: The initial enrichment scale-out target is consumer lag above 500 messages per effective replica/task; the Risk Alert Service uses a lower initial target of 200 because delayed risk evaluation is more sensitive. Thresholds must be tuned from load-test evidence.

FR-08.3: Scale-in must use cooldown/stabilisation to prevent thrashing; the initial EKS cooldown is 120 seconds. ECS cooldown is configured independently through Application Auto Scaling.

FR-08.4: Minimum compute: 1 replica/task. Maximum effective concurrency must not exceed useful Kafka partition parallelism unless an ADR explains the reason.

FR-08.5: Scale-out must produce measurable lag reduction within 90 seconds of new instances/tasks becoming ready.

### FR-09: Deployment

FR-09.1: A Docker Compose file must bring up the complete local stack including Kafka, Schema Registry, Kafka UI, PostgreSQL, Redis, Prometheus, Grafana, Loki, and OpenTelemetry Collector with a single command.

FR-09.2: ECS task definitions and service configurations must deploy all consumer services to Fargate without manual configuration.

FR-09.3: Amazon MSK must be used as the Kafka cluster in the ECS and EKS deployments. Self-managed brokers are for local development only.

FR-09.4: Kubernetes manifests and a Helm chart must be provided for EKS deployment. The Helm chart must support separate values files for dev, staging, and production environments.

FR-09.5: Application workloads must use workload identity and temporary credentials wherever supported. Secrets Manager is used only for unavoidable secrets and those secrets must not appear in plaintext environment files, task definitions, Kubernetes manifests, CI logs, or source control.

---


### FR-10: Instrument Reference Data

FR-10.1: The platform must maintain a synthetic instrument master and publish instrument-reference events to `reference-data.instruments`.

FR-10.2: Each instrument record must include ticker, exchange, synthetic ISIN, security type, currency, sector, shares outstanding, and market capitalisation or the inputs required to derive market capitalisation.

FR-10.3: `reference-data.instruments` must be a compacted topic so the latest state of each instrument can be reconstructed.

FR-10.4: The Trade Enrichment Service must consume or cache reference data rather than hard-code instrument metadata.

FR-10.5: Reference-data changes must be versioned and traceable to the producing workload identity.

### FR-11: Position and Exposure Read Model

FR-11.1: A Position & Exposure Service must consume `trades.enriched` and maintain a real-time read model by account, trader, and ticker.

FR-11.2: The service must calculate at minimum net position, gross buy quantity, gross sell quantity, market value, realised/unrealised synthetic exposure where applicable, and last update timestamp.

FR-11.3: Position updates must be idempotent by `tradeId`; reprocessing an event must not double-count a position.

FR-11.4: The service must publish periodic or material-change snapshots to `positions.snapshots` and expose a read-only API for authorised risk/compliance users.

FR-11.5: The service must support rebuilding the complete read model from Kafka history and must expose a reconciliation result after rebuild.

### FR-12: Risk Rule Governance

FR-12.1: Risk rules must be versioned entities with states `DRAFT`, `PENDING_APPROVAL`, `ACTIVE`, `RETIRED`, and `REJECTED`.

FR-12.2: A material rule change must follow maker-checker control: the identity that proposes the change cannot approve the same change.

FR-12.3: An approved rule may have an effective-from timestamp. The Risk Alert Service must activate only approved rules at or after the effective timestamp.

FR-12.4: Every rule lifecycle transition must publish an immutable event to `risk-rules.events` containing rule version, action, actor identity, approver identity when applicable, timestamp, reason, and correlation ID.

FR-12.5: The platform must support rollback to a previously approved rule version without modifying historical versions.

FR-12.6: The current active rule set must be reconstructable from the event history.

### FR-13: Alert Case Lifecycle

FR-13.1: Every WARNING or CRITICAL risk alert must create or update an investigation case.

FR-13.2: Case states must include `OPEN`, `ACKNOWLEDGED`, `INVESTIGATING`, `CLOSED`, and `FALSE_POSITIVE`.

FR-13.3: Case state changes must require an authenticated operator and a reason code; CRITICAL case closure must require a role authorised for critical-risk disposition.

FR-13.4: Case changes must be published to `alert-cases.events` and preserved in the audit archive.

FR-13.5: The platform must expose read-only case timelines that combine the triggering event, evaluated rule version, alert, operator actions, and final disposition.

### FR-14: Control and Reconciliation Service

FR-14.1: A Control & Reconciliation Service must compare produced/consumed Kafka event counts and offset ranges with archived audit manifests.

FR-14.2: The service must detect missing offsets, duplicate audit records, incomplete partition ranges, and unexplained count differences.

FR-14.3: Control results must be published to `controls.reconciliation` with status `PASS`, `WARNING`, or `FAIL`.

FR-14.4: A failed reconciliation must generate a CRITICAL operational alert and must be visible on the business/control dashboard.

FR-14.5: Reconciliation evidence must be retained with the audit data and be queryable by date, topic, partition, and control-run ID.

### FR-15: Administrative Security Control Plane

FR-15.1: Privileged functions must be exposed through a dedicated administrative control plane, not through unauthenticated service endpoints.

FR-15.2: Human operators must authenticate through an OIDC-compatible identity provider. The local reference implementation uses Keycloak; the cloud architecture must remain compatible with enterprise IdPs.

FR-15.3: The minimum operator roles are `PlatformAdmin`, `RiskMaker`, `RiskChecker`, `Operator`, `ComplianceAuditor`, and `SecurityAuditor`.

FR-15.4: The following operations must require explicit authorization: risk-rule proposal/approval, DLQ replay, case disposition, audit export, configuration change, and privileged diagnostic actions.

FR-15.5: Every privileged action must record actor identity, role/attributes, action, target, reason, request ID, policy decision, timestamp, and outcome.

FR-15.6: The control plane must publish security-relevant actions and denials to `security.events`.

### FR-16: Audit Integrity and Evidence

FR-16.1: Each S3 audit flush must produce a sidecar audit manifest containing event count, topic/partition offset ranges, object key, object digest, schema versions, write timestamp, and Audit Service workload identity.

FR-16.2: In the AWS security profile, the manifest digest must be digitally signed using an asymmetric KMS signing key. Verification must be supported independently from event processing.

FR-16.3: The audit data and manifests must be stored under retention controls that prevent normal application identities from deletion or overwrite.

FR-16.4: An integrity-verification command or API must verify object digest, manifest signature, and expected partition-offset coverage for a selected audit batch.

FR-16.5: Signature verification failure must create a CRITICAL security event and reconciliation failure.



### FR-17: Legacy-to-Streaming CDC Migration

FR-17.1: A mock legacy PostgreSQL database must contain a `legacy_trade` table representing a batch-era source system.

FR-17.2: An actual Debezium PostgreSQL connector must capture inserts/updates/deletes from the legacy source and publish change events to `legacy.trades.cdc`. A hand-written "Debezium-style" simulator is not sufficient for the validation profile.

FR-17.3: A Migration Normalizer service must transform CDC envelopes into the canonical `TradeEvent` schema and publish to `trades.raw` with migration provenance fields sufficient to trace the canonical event back to the source table/key/LSN or equivalent connector position.

FR-17.4: Initial snapshot/backfill and ongoing CDC must be supported as separate phases. The platform must demonstrate a strangler-style coexistence period in which live synthetic producers and the legacy CDC source feed the canonical event pipeline concurrently.

FR-17.5: Migration validation must include duplicate suppression/idempotency, count reconciliation, schema compatibility, restart from connector offsets, and a documented rollback/cutover procedure.

### FR-18: Reconciliation Observation Stream

FR-18.1: A Reconciliation Simulator must emit synthetic `ReconciliationObservationEvent` records to `reconciliation.observations`.

FR-18.2: Supported scenarios must include duplicate settlement/reference observations, internal-vs-external amount drift, conflicting state transitions, missing participant/leg observations, and benign timing/rounding cases.

FR-18.3: The simulator observes and compares synthetic records only. It must not model or execute money movement, settlement finality, or ledger correction.

FR-18.4: Reconciliation observations must carry provenance for each compared source, observation timestamps, reference/entity identifiers, and completeness state so that timing uncertainty is distinguishable from a proven mismatch.

### FR-19: Deterministic Anomaly Candidate Service

FR-19.1: The high-throughput streaming plane must remain deterministic. LLM inference must not execute synchronously for every `trades.enriched` event.

FR-19.2: An `anomaly-candidate-service` must consume selected outputs from `trades.enriched`, `notifications.alerts`, `reconciliation.observations`, corporate-action/reference context, and position/exposure signals and publish bounded `AnomalyCandidateEvent` records to `anomaly.candidates`.

FR-19.3: Candidate generation must use deterministic rules/statistical screening and must record the signals that caused the event to be promoted for investigation.

FR-19.4: Candidate generation must be configurable by category, severity, sampling rate, and queue-pressure thresholds. Normal-event sampling may be enabled for evaluation but must be independently bounded.

FR-19.5: Failure or backlog in the agentic investigation plane must never block or backpressure the deterministic risk and audit hot path.

### FR-20: Reconciliation & Anomaly Investigation Agent

FR-20.1: The agent service must consume `anomaly.candidates`, not the full high-rate market stream.

FR-20.2: For each candidate, the agent may use explicitly registered read-only tools such as `ledger-lookup`, reference/calendar lookup, position/history lookup, and precedent retrieval.

FR-20.3: The only direct state-mutating agent tool is `flag-for-review`, which may create or update a **proposed** review case. The agent has no credential capable of modifying a financial ledger, changing risk rules, replaying DLQs, or executing remediation.

FR-20.4: Each investigated candidate must produce a structured decision with: outcome (`FLAG`, `NO_FLAG`, `ESCALATE`, or `INCONCLUSIVE`), severity, confidence, reason codes, evidence references, tool-status summary, cited precedent where applicable, and a concise human-readable narrative.

FR-20.5: The agent must distinguish missing evidence/tool failure from evidence of normality. If a required tool fails after bounded retry, the correct behavior is `ESCALATE`, not guessing.

FR-20.6: A separate critique pass may revise the draft once. It must be triggered for medium/low-confidence decisions, tool/evidence gaps, high-impact candidate categories, and a configurable sample of high-confidence decisions. The critique loop is bounded to one revision.

FR-20.7: The platform must record a structured **decision/evidence trace** sufficient to audit inputs, tool calls/results, retrieved precedents, output, confidence, critique result, model/prompt/tool versions, token usage, and timestamps. Raw hidden chain-of-thought is neither required nor persisted.

FR-20.8: Event payloads, retrieved text, graph content, and tool outputs are untrusted data. The agent runtime must prevent them from redefining system instructions or expanding tool permissions.

### FR-21: Human Review & Feedback

FR-21.1: Every agent-created flag/escalation must enter a human review queue before any downstream remediation workflow.

FR-21.2: Reviewers must be authenticated and authorized through the existing administrative control plane. Reviewer decisions must include verdict, reason, reviewer identity, timestamp, and optional notes.

FR-21.3: For this portfolio system, an approved remediation is represented only by a `remediation.requested` synthetic event or case-state transition. No actual financial system is modified.

FR-21.4: Human review decisions must be emitted as `HumanReviewDecisionEvent` records, archived, and made available to the feedback/evaluation pipeline.

### FR-22: Precedent Graph

FR-22.1: Neo4j must hold a derived case-history graph containing only bounded precedent data required for investigation. PostgreSQL remains the source of truth for case/review state.

FR-22.2: The graph must support at least Event, Instrument, Counterparty, AnomalyPattern, Resolution, and ReviewCase nodes plus explicit relationships.

FR-22.3: Graph synchronization must be event-driven from reviewed-case events and idempotent. If Neo4j is unavailable, case processing must degrade to no-precedent/flat fallback rather than block the deterministic platform.

FR-22.4: Precedent retrieval must return evidence identifiers and relationship paths used in the decision so that the citation is auditable.

### FR-23: Evaluation & Regression Harness

FR-23.1: A versioned golden dataset must seed with the supplied 15 cases covering clear anomalies, false-positive traps, baseline normal cases, ambiguity, tool failure, and reconciliation/state anomalies.

FR-23.2: Golden cases must use a machine-evaluable normalized outcome schema. Compound text labels must not be the only source of truth.

FR-23.3: False-negative, false-positive, escalation/tool-failure correctness, and confidence/calibration results must be reported separately. A single blended "accuracy" score is insufficient.

FR-23.4: At the initial 15-case size the dataset is a regression suite, not a statistically representative production-accuracy benchmark. Reports must state this limitation.

FR-23.5: Exact fields/outcomes must use deterministic assertions where possible. LLM-as-judge may be used only for narrative/evidence-quality dimensions that cannot be deterministically scored, and the judge rubric/prompt/model must be versioned and calibrated against human review before it can block CI.

FR-23.6: Changes to agent prompts, model, tool definitions, retrieval logic, graph schema, decision schema, or candidate routing must trigger the relevant evaluation suite in CI.

FR-23.7: An adversarial set must test prompt injection through event/graph/tool payloads, fabricated tool results, unauthorized tool requests, excessive agency, evidence omission, and attempts to bypass human approval.

FR-23.8: The project must prove the gate by intentionally introducing at least one known regression and capturing the blocked CI result.

### FR-24: AI Decision Audit & Cost Evidence

FR-24.1: Every agent invocation must produce an exportable audit document linking source candidate, deterministic trigger evidence, tool calls/results, precedent references, draft/final structured decision, critique result where present, human verdict, model/prompt/tool versions, and timestamps.

FR-24.2: Agent token usage, latency, model/provider identifier, cache usage where available, tool-call count, and estimated invocation cost must be measurable per candidate and aggregated by candidate category.

FR-24.3: AI audit evidence must join the same immutable audit pipeline as other control evidence without exposing credentials, secret values, hidden chain-of-thought, or unnecessarily sensitive payloads.

### FR-25: Cost & Capacity Model

FR-25.1: The platform must attribute infrastructure cost separately for streaming ingestion, enrichment/risk, audit/storage, observability, CDC migration, and AI investigation.

FR-25.2: Load-test results must report throughput/latency together with observed or estimated run cost and resource utilization so performance claims are not separated from economic trade-offs.

FR-25.3: The AI plane must have independent rate/budget limits and queue backpressure. Exceeding the AI budget must degrade to human review/delayed investigation without affecting deterministic risk processing.

---

## Non-Functional Requirements

### NFR-01: Throughput

NFR-01.1: The platform must sustain 50,000 events per second end-to-end from producer to audit archive under sustained load for a minimum of 3 minutes.

NFR-01.1a: The 50,000 events/second objective must be attempted under the production durability profile (`acks=all`, idempotent producer, replication factor 3, `min.insync.replicas=2`). If the ephemeral validation environment cannot meet the target, the project must publish the measured sustainable ceiling and bottleneck; an `acks=1` ceiling run may be reported separately but must not substitute for the production result.

NFR-01.2: The Trade Enrichment Service must process a single event within 10ms at p99 under nominal load of 10,000 events per second.

NFR-01.3: The Risk Alert Service must evaluate a single trade within 5ms at p99 under nominal load.

NFR-01.4: The total pipeline latency from producer publish to risk alert available on the notification topic must be below 200ms at p99 under nominal load.

### NFR-02: Reliability

NFR-02.1: No event must be permanently lost. Events that fail consumer processing must land in the DLQ. Events in the DLQ must be replayable.

NFR-02.2: The platform must survive the loss of one Kafka broker without message loss or consumer outage, with rebalancing completing within 30 seconds.

NFR-02.3: The platform must survive the loss of one consumer service instance without message loss. The active orchestrator must restore desired capacity within 60 seconds after failure is detected (Kubernetes/EKS controller for pods; ECS service scheduler for tasks).

NFR-02.4: The Audit Service must achieve at-least-once delivery to S3. Idempotent consumers in downstream analytics systems must handle duplicates.

### NFR-03: Scalability

NFR-03.1: Consumer services must scale horizontally without code changes. Adding a new consumer instance must improve throughput linearly up to the number of topic partitions.

NFR-03.2: The deployment-specific autoscaler must demonstrate measurable scale-out within 90 seconds of a sustained consumer lag spike: KEDA for EKS and ECS Service Auto Scaling/Application Auto Scaling for ECS.

NFR-03.3: Kafka topics must be configured with a minimum of 12 partitions to allow scaling to 12 consumer instances per group.

NFR-03.4: The platform architecture must support adding new event types by adding new Avro schemas and consumer services without modifying existing services.

### NFR-04: Observability

NFR-04.1: Any event must be traceable end-to-end from producer to audit archive using a single correlationId query in Grafana Loki.

NFR-04.2: Consumer lag must be visible in Grafana within 15 seconds of the lag event occurring.

NFR-04.3: A service restart must be detectable in the pipeline health dashboard within 30 seconds.

NFR-04.4: The mean time to detect a pipeline failure must be under 2 minutes with the configured Prometheus alert rules.

### NFR-05: Security and Identity

NFR-05.1: All cloud inter-service communication must be encrypted in transit. TLS 1.2 or higher is required; plaintext Kafka, database, cache, administration, and telemetry traffic is not permitted in cloud profiles.

NFR-05.2: Long-lived AWS access keys are prohibited for application workloads. ECS services must use dedicated task roles; EKS workloads must use dedicated Kubernetes service accounts mapped to EKS Pod Identity (or an explicitly documented alternative when required).

NFR-05.3: Every independently deployable workload must have a distinct identity and least-privilege permission set. Shared wildcard service roles are prohibited.

NFR-05.4: Amazon MSK cloud deployments must use IAM authentication and IAM authorization. Topic, consumer-group, cluster, and transactional permissions must be scoped per workload identity. Kafka ACLs are reserved for non-IAM/local security profiles.

NFR-05.5: The local stack must support a `strict-security` profile that enables authenticated Kafka access and TLS so that service-identity failures can be tested without AWS.

NFR-05.6: MSK, S3 audit data, RDS, ECR, and other persistent cloud stores must use encryption at rest. Customer-managed KMS keys must be used for the audit archive and signing use cases; other services may use AWS-managed keys when explicitly justified in an ADR.

NFR-05.7: The S3 audit bucket must enable Block Public Access, versioning, SSL-only access, and S3 Object Lock. Application identities must not have delete, retention-bypass, or bucket-policy administration privileges.

NFR-05.8: Human administrative access must be federated through an OIDC-compatible IdP and require MFA in the cloud/reference enterprise profile. Shared human accounts are prohibited.

NFR-05.9: Administrative authorization must enforce the defined platform roles and separation-of-duties rules. A RiskMaker must not approve their own risk-rule change.

NFR-05.10: Secrets Manager is used only for secrets that cannot be replaced by workload identity or temporary credentials. Secrets must not appear in source code, Git history, plaintext environment files, task definitions, Kubernetes manifests, build logs, or telemetry.

NFR-05.11: Unavoidable database/cache secrets must be rotated automatically where supported. The cloud design must evaluate and prefer IAM-based database/cache authentication where it provides acceptable operational characteristics.

NFR-05.12: Workloads must run in private subnets or private cluster networking. Public ingress to Kafka, databases, caches, Schema Registry, telemetry backends, and control-plane services is prohibited. Security groups must be service-specific and egress must be intentionally scoped.

NFR-05.13: Administrative AWS API activity must be captured by CloudTrail. Data-event logging must be enabled for security-critical resources where evidence requirements justify the additional cost.

NFR-05.14: IAM policies must be validated in CI using IAM Access Analyzer policy validation or equivalent checks. Cloud test runs must include a least-privilege review using actual access activity before policies are considered final.

NFR-05.15: Authorization denials, failed authentications, role/privilege usage, risk-rule approvals, DLQ replays, audit integrity failures, and key/secret lifecycle events must be emitted as structured security telemetry.

NFR-05.16: Sensitive identifiers must not be copied into high-cardinality logs unnecessarily. No credentials, tokens, secret values, raw authorization headers, or private-key material may be logged.

NFR-05.17: Containers must run as non-root, with a read-only root filesystem where technically feasible, no privileged mode, no host networking, and Linux capabilities dropped unless explicitly required.

NFR-05.18: Every trust boundary and privileged workflow must have a documented threat model. Threat-model changes are required when a new identity provider, admin endpoint, data store, cross-account connection, or external integration is added.

NFR-05.19: Security requirements must include negative tests. At minimum, each workload must be tested for one allowed and multiple explicitly denied cross-service/resource actions.

NFR-05.20: The EKS advanced-security profile must demonstrate workload attestation and short-lived service identity using SPIFFE/SPIRE for at least the Trade Enrichment, Risk Alert, and Audit services, while AWS API permissions continue to use AWS workload identity.

### NFR-06: Maintainability

NFR-06.1: Every architecture decision with more than one viable alternative must be documented in an ADR under `docs/adr/`.

NFR-06.2: A runbook must be maintained covering at least: consumer lag spike, DLQ depth alarm, broker loss, schema deployment/rollback, consumer restart, authorised DLQ replay, alert investigation, EKS KEDA scaling verification, ECS autoscaling verification, MSK broker replacement, workload-identity failure, operator-access failure, audit-integrity failure, and reconciliation failure.

NFR-06.3: CI must run on every pull request and must include: compilation, unit tests, integration tests with embedded Kafka, schema compatibility check, and Docker image build.

NFR-06.4: All Grafana dashboards must be version-controlled as JSON. No dashboard changes without a corresponding pull request.

### NFR-07: Cost

NFR-07.1: The local development environment must run entirely on Docker Compose with no cloud dependencies.

NFR-07.2: The ECS Fargate deployment must use Fargate Spot for non-critical services to reduce compute cost.

NFR-07.3: Every cloud validation run must be ephemeral and have a pre-run cost estimate. A configurable lab cost envelope (`LAB_BUDGET_USD`) must be documented in the run plan; the deployment must be torn down automatically at the end of validation.

NFR-07.4: AWS Budgets/alerts must be configured for the validation account, with a budget action that can deny additional provisioning where practical. Deterministic teardown automation is the authoritative cost-control mechanism; AWS Budgets must not be described as a universal hard stop for running services.

---


### NFR-08: Security Observability and Detection

NFR-08.1: A dedicated security dashboard must show authentication failures, authorization denials, privileged operations, denied Kafka/API access, IAM-related failures, audit-integrity status, and security-event rate.

NFR-08.2: A CRITICAL alert must fire on audit signature verification failure, unexpected public exposure of a protected resource, repeated denied access from a workload, privileged replay without an approved reason, and reconciliation failure indicating potential event loss.

NFR-08.3: Security signals must preserve actor/workload identity, target resource, action, decision, reason, trace/correlation ID, and environment without logging credentials.

NFR-08.4: Mean time to detect a simulated security-control failure must be under 2 minutes for controls represented by real-time metrics/events.

### NFR-09: Software Supply Chain Security

NFR-09.1: CI must perform secret scanning, dependency vulnerability scanning, SAST, IaC/security-policy validation, container-image scanning, and SBOM generation.

NFR-09.2: Releases must be built from pinned source revisions and immutable image digests. Deployment manifests must reference image digests in the production/security validation profile.

NFR-09.3: Container images used in cloud validation must be signed by the trusted CI pipeline using an OCI-compatible signing mechanism; signature verification must be part of the deployment gate in the EKS advanced-security profile.

NFR-09.4: Critical/high vulnerabilities must fail the release gate unless an explicit, time-bounded risk acceptance is committed under `docs/security/exceptions/`.

### NFR-10: Data Classification and Protection

NFR-10.1: Every event schema and data store must be assigned a classification: `PUBLIC`, `INTERNAL`, `CONFIDENTIAL`, or `RESTRICTED`.

NFR-10.2: Synthetic `traderId` and `accountId`, alert investigations, risk rules, audit evidence, and security events must be treated as `RESTRICTED` for architecture/testing purposes even though the project contains no real PII.

NFR-10.3: Data classification must influence access policy, logging/redaction, encryption-key choice, retention, and export permissions.

NFR-10.4: The platform must provide a documented retention schedule for Kafka topics, operational databases, security events, cases, and immutable audit objects.

### NFR-11: Recovery and Security Resilience

NFR-11.1: Recovery procedures must include loss/rotation of a workload identity, signing-key disablement, compromised operator account, revoked IdP session, compromised secret, and accidental policy lockout.

NFR-11.2: Break-glass access, if implemented, must be time-bounded, separately auditable, excluded from normal developer roles, and followed by mandatory review.

NFR-11.3: A workload identity or authorization-policy revocation must become effective for new AWS API/Kafka access without redeploying unrelated services.

NFR-11.4: The platform must document RTO/RPO assumptions separately for streaming processing, operational read models, and immutable audit data.



### NFR-12: Agent Safety and Permission Boundaries

NFR-12.1: Agent workloads must use an explicit non-human workload identity and must not reuse human operator credentials.

NFR-12.2: Tool authorization must be enforced outside the LLM by code/policy. The model cannot grant itself tools, broader resource scope, or higher privileges by generating text.

NFR-12.3: The agent must have hard limits for wall-clock duration, model/tool iterations, tool-call count, and per-invocation budget. Limit exhaustion must fail safely to `ESCALATE`.

NFR-12.4: Tool arguments and outputs must be schema-validated. Write-capable tools must be allowlisted individually and use separate authorization policies from read-only tools.

NFR-12.5: Prompt injection or malicious instructions embedded in financial event data, precedent text, or tool results must not alter system policy or human-approval requirements.

### NFR-13: AI Reliability and Evaluation

NFR-13.1: Model, prompt, system instruction, tool schema, graph/retrieval configuration, decision schema, and evaluation rubric versions must be recorded for every eval run and agent decision.

NFR-13.2: CI regression thresholds must be category-aware. Any newly introduced false negative on designated critical anomaly cases is a release blocker unless explicitly waived in a time-bounded, reviewed exception.

NFR-13.3: Golden-set growth must preserve purposeful coverage of false-positive traps, ambiguity, tool failure, security/adversarial cases, and reconciliation state conflicts rather than merely reflecting production class frequency.

NFR-13.4: Human-reviewed cases may become evaluation examples only after data is synthetic/public-safe or appropriately sanitized. The open-source repository must never ingest employer/client production data.

### NFR-14: Agent Observability

NFR-14.1: Agent spans must continue the originating Kafka/candidate trace where feasible, with a link when asynchronous queue boundaries make parent/child semantics misleading.

NFR-14.2: Telemetry must capture model/provider identifier, latency, token counts, cache counters where available, tool-call names/status, candidate category, decision outcome, confidence, critique-trigger reason, and cost. Prompts/tool payloads must be redacted or omitted by default.

NFR-14.3: Dashboards must expose candidate backlog, time-to-review-case, agent failure rate, escalation rate, false-positive/false-negative regression results, per-category cost, and tool dependency failures.

NFR-14.4: An agentic-plane outage must be detectable without causing the deterministic risk/audit health dashboard to report the hot path as unavailable.

### NFR-15: Performance Isolation

NFR-15.1: The existing 50,000-events/second streaming load objective applies to the deterministic streaming plane only.

NFR-15.2: Agent throughput is measured independently in candidates/second and time-to-case. Agent backlog must be bounded and observable.

NFR-15.3: At peak streaming load, the agentic plane may lag or shed low-priority investigation work according to policy, but it must not increase deterministic risk-alert p99 latency beyond the existing target.

### NFR-16: CDC Migration Reliability

NFR-16.1: Restarting the Debezium connector must not create unbounded duplicates or lose committed source changes.

NFR-16.2: CDC source credentials must be dedicated and least privilege. The connector may read/capture only configured legacy tables and write only to designated CDC topics.

NFR-16.3: Migration cutover evidence must include source/canonical counts, deduplication totals, incompatible-record totals, lag, connector offset position, and rollback readiness.

NFR-16.4: A controlled failure test must stop/restart the connector during live source updates and verify recovery from stored offsets.

---

## Constraints

The platform must be implemented in Java 25 with Spring Boot for all producer and consumer services.

Infrastructure as code must use AWS CDK for ECS/cloud AWS resources and Helm for EKS deployment. Security policies and identity mappings must be represented as code and version-controlled.

Kafka client library must be the official Apache Kafka client wrapped by Spring Kafka.

Schema serialisation must use Avro. JSON serialisation is not permitted for primary financial event payloads; JSON is permitted for control-plane HTTP APIs, structured logs, and audit manifest metadata.

No application workload may require a long-lived AWS access key. Secrets Manager is reserved for unavoidable third-party/service credentials and rotated secrets.

Administrative APIs must not be exposed anonymously.

All Docker images must be based on Amazon Corretto 25 unless an ADR approves another minimal trusted base image.

The default developer workflow remains local-first. Cloud security validation is intentionally ephemeral and must be reproducible from IaC.

The deterministic streaming plane must remain deployable and testable without any LLM provider, Neo4j, or agent runtime. AI/graph dependencies are optional investigation-plane components, not hard dependencies of trade/risk/audit processing.

---

## Assumptions

The simulator-generated market data and financial identities are synthetic and sufficient for functional, performance, security, and control testing. Licensed real-time feeds from NSE, BSE, or commercial providers are not required.

The project is a financial-platform reference implementation, not a broker, exchange member, depository participant, payment system, or production regulatory reporting system.

The EKS validation environment is ephemeral. KEDA is a Kubernetes component and is used only in EKS/local Kubernetes profiles; ECS uses Amazon ECS Service Auto Scaling / Application Auto Scaling with lag-derived CloudWatch metrics.

Cloud validation cost must be estimated before each run and controlled through budgets, alerts, deny-new-provisioning actions where practical, and deterministic teardown automation. AWS Budgets is not assumed to be a universal hard-stop mechanism for all running services.

SPIFFE/SPIRE is an advanced workload-identity learning profile and is not required to replace AWS IAM for AWS API authorization.
