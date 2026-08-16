# financial-event-streaming-platform

Security-first Kafka financial event streaming, risk-control and legacy-CDC-migration platform, with
a bounded, human-gated LLM investigation plane layered on asynchronously.

Portfolio and reference implementation. No real money movement, no real PII, no regulatory reporting.
All financial identifiers and reconciliation records are synthetic.

**Current state (2026-08-16): design-complete, implementation not started.**
`docs/` holds the full specification. `src/` holds one `@SpringBootApplication` and a `contextLoads()`
test. There are no producers, consumers, entities, repositories, controllers, Avro schemas or Flyway
migrations. Read the actual source before assuming any component exists.

## Stack

Java 25 (LTS, pinned via Gradle toolchain), Spring Boot 4.1.0, Gradle 9.5.1.
Apache Kafka with Avro and Confluent Schema Registry, PostgreSQL, Redis, S3 with Object Lock,
Neo4j (derived precedent graph), Debezium PostgreSQL connector, OpenTelemetry, Prometheus, Grafana, Loki.
AWS CDK (Java) for ECS Fargate, Helm for EKS.

## Build (Gradle wrapper, always from repo root)

```bash
./gradlew build                                              # compile + test
./gradlew test                                               # tests only
./gradlew test --tests 'dev.engnotes.SomeTest'               # single class
./gradlew test --tests 'dev.engnotes.SomeTest.someMethod'    # single method
./gradlew bootRun                                            # needs external Kafka/Postgres
./gradlew bootTestRun                                        # runs against Testcontainers
```

`./gradlew test` requires a running Docker daemon. `TestcontainersConfiguration` starts Kafka
(`apache/kafka-native`), PostgreSQL, Zipkin and the Grafana LGTM stack.

There is no Maven build here. Ignore any `./mvnw` reference. No formatter and no linter are
configured; `check` maps to `test` only. Spotless is not on this build.

## Architecture: five planes and one invariant

1. **Ingestion and migration**: synthetic simulators plus Debezium CDC from a mock legacy PostgreSQL.
2. **Deterministic streaming**: enrichment, risk evaluation, position and exposure read models.
   Targets 50k events/sec and sub-200ms risk latency.
3. **Audit and control**: immutable S3 Object Lock evidence, KMS-signed manifests, reconciliation.
4. **Candidate and case**: deterministic anomaly screening, alert cases, maker-checker rule governance.
5. **Agent investigation**: bounded LLM investigation, typed read-only tools, Neo4j precedent graph,
   human review.

**The invariant that governs most design choices:** Plane 5 is asynchronous and optional. Provider
outage, throttling, graph unavailability or budget exhaustion in the agent plane must never block or
add an availability dependency to Planes 1 through 3. The two planes carry separate SLOs. The
50k/sec figure is a deterministic-streaming target and never a claim about LLM throughput.

## Module layout (ADR-028)

Independently deployable services as Gradle modules, grouped by plane. Package root
`dev.engnotes.fes.{service}`.

```text
contracts/              Avro schemas + generated types, dev.engnotes.fes.events
platform-common/        Kafka defaults, idempotency keys, DLQ publisher, logging, OTel conventions
services/ingestion/     trade-producer, market-data-simulator, corporate-action-producer,
                        reference-data-service, migration-normalizer
services/streaming/     market-data-cache-projector, trade-enrichment-service,
                        risk-alert-service, position-exposure-service
services/audit/         audit-service, reconciliation-service
services/control/       alert-case-service, risk-rule-governance-service,
                        anomaly-candidate-service, reconciliation-simulator, admin-control-plane
services/agent/         agent-investigation-service, human-review-service, precedent-sync-service
```

Modules land per README phase. Do not create empty modules ahead of the phase that needs them.

## Which docs are authoritative

`docs/requirements-v1.2.md`, `docs/architecture-v1.2.md`, `docs/specification-v1.2.md`, and the ADRs
under `docs/adr/`. Anything under `docs/archive/` is superseded: do not implement from it and do not
cite it.

Avro schemas are inline in `docs/specification-v1.2.md` under namespace `dev.engnotes.fes.events`,
and land as files in `contracts/src/main/avro/`. The Kafka topic inventory with retention and
partition counts is in `docs/architecture-v1.2.md` and mirrored in `.claude/rules/kafka.md`.

## Always

- At-least-once delivery, idempotent producers, explicit offset commits, deterministic idempotency keys
- Avro for financial event payloads; JSON only for control-plane HTTP, logs and audit manifests
- DLQ topic naming `{original-topic}.dlq`, lowercase
- Consumer group name equals service name, no suffix
- Records for data carriers, sealed interfaces plus exhaustive switch for closed domains
- Every service: its own workload identity, its own schema, its own consumer group, its own image
- Every privileged action: OIDC identity, policy decision, reason, and a `SecurityEvent` on both
  ALLOW and DENY
- Run `scripts/sync-agent-config.sh` after editing anything under `.claude/skills/` or `.claude/agents/`

## Never

- Introduce Kafka transactions, or describe the system as exactly-once (ADR-019)
- Call an LLM per event on the hot path (ADR-021)
- Give the agent any credential beyond read-only tools plus `flag-for-review` proposals (ADR-023)
- Treat Neo4j as a source of truth, or block deterministic processing on it (ADR-022)
- Open an event-type-wide circuit breaker for a poison record (ADR-027)
- Fall back to a synchronous simulator call on a market-cache miss (ADR-027)
- Persist raw chain-of-thought (ADR-025)
- Simulate CDC in application code instead of running the real Debezium connector (ADR-020)
- Write an accuracy claim from the 15-case golden dataset (ADR-024)
- Let an ingestion, streaming or audit module depend on agent-plane code (ADR-026, ADR-028)
- Assume a service, schema or migration exists. Read the source first.

## When Compressing Context, Keep

- Which service module is being modified, and which plane it belongs to
- Decisions made about architecture or approach this session
- Test results from the last run
