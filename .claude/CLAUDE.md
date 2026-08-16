# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state: design-complete, implementation not started

`docs/` holds roughly 5,500 lines of specification describing about 20 services. `src/` holds one
`@SpringBootApplication` class and a `contextLoads()` test. The repository has zero commits.

Nothing in the architecture docs is built yet. There are no producers, consumers, entities,
repositories, controllers, Avro schemas, Flyway migrations, or `db/migration/` directory. Read the
actual source before assuming any component exists.

## Build and test

Gradle wrapper, not Maven. Ignore any `./mvnw` reference in this repo; it belongs to a different project.

```bash
./gradlew build                                              # compile + test
./gradlew test                                               # tests only
./gradlew test --tests 'dev.engnotes.SomeTest'               # single class
./gradlew test --tests 'dev.engnotes.SomeTest.someMethod'    # single method
./gradlew bootRun                                            # run app (needs external Kafka/Postgres)
./gradlew bootTestRun                                        # run app with Testcontainers wired in
```

- Toolchain: Java 26, Gradle 9.5.1, Spring Boot 4.1.0.
- `./gradlew test` requires a running Docker daemon. `TestcontainersConfiguration` starts Kafka
  (`apache/kafka-native`), PostgreSQL, Zipkin, and the Grafana LGTM stack, all on `:latest` tags.
- `bootTestRun` launches the app against those same containers via
  `TestFinancialEventStreamingPlatformApplication`.
- No formatter and no linter are configured. `check` maps to `test` only. Contrary to the comment in
  `.claude/hooks/auto-format.sh`, Spotless is not on the build.

## Which docs are authoritative

`docs/` contains superseded first-pass documents alongside the current set. Always read the v1.2 files:

| Use | Not |
| --- | --- |
| `docs/requirements-v1.2.md` | `docs/requirements.md` |
| `docs/architecture-v1.2.md` | `docs/architecture.md` |
| `docs/specification-v1.2.md` | `docs/spec.md` |

`docs/adr/` holds the binding decisions. Two files share the number ADR-022
(`ADR-022-graph-layer-detailed.md` and `ADR-022-neo4j-precedent-graph.md`); the detailed one is the
fuller treatment. `docs/adr-001-graph-layer.md` at the docs root is an earlier draft of the same topic.

Avro event schemas live inline in `docs/specification-v1.2.md` under namespace `dev.engnotes.fes.events`.
The Kafka topic inventory with retention and partition counts is `docs/architecture-v1.2.md:120-152`.

## Architecture: five planes and the isolation invariant

1. Ingestion and migration: synthetic simulators plus Debezium CDC from a mock legacy PostgreSQL.
2. Deterministic streaming: enrichment, risk evaluation, position and exposure read models. Targets
   50k events/sec and sub-200ms risk latency.
3. Audit and control: immutable S3 Object Lock evidence, signed manifests, reconciliation.
4. Candidate and case: deterministic anomaly screening, alert cases, maker/checker rule governance.
5. Agent investigation: bounded LLM investigation, typed read-only tools, Neo4j precedent graph,
   human review.

**The invariant that governs most design choices:** Plane 5 is asynchronous and optional. Provider
outage, throttling, graph unavailability, or budget exhaustion in the agent plane must never block or
add an availability dependency to Planes 1 through 3. The two planes carry separate SLOs; the
50k/sec figure is a deterministic-streaming target and never a claim about LLM throughput.

## Binding invariants from the ADRs

- At-least-once delivery with idempotent producers, explicit offset management, and deterministic
  idempotency keys. Do not introduce Kafka transactions or describe the system as exactly-once (ADR-019).
- Deterministic screening promotes a bounded subset to `anomaly.candidates`. Never call an LLM per
  event on the hot path (ADR-021).
- Agent tools are read-only plus `flag-for-review`, which can only create a proposal. The model holds
  no credential for ledger mutation, rule approval, DLQ replay, IAM changes, or remediation. Safety is
  enforced by authorization outside the model, not by prompt instruction (ADR-023).
- Neo4j is a derived, rebuildable precedent graph. PostgreSQL stays authoritative for cases,
  decisions, and feedback. The agent must degrade when the graph is unavailable (ADR-022).
- Poison records are quarantined per record after bounded retry. Never open an event-type-wide
  circuit breaker; breakers protect calls to failing dependencies only (ADR-027).
- Redis market state is projected from `market-data.ticks`. Trade enrichment must not fall back to a
  synchronous simulator call on cache miss (ADR-027).
- Persist structured decision and evidence traces (model, prompt and tool versions, tool calls,
  precedent ids, verdict, latency, tokens, cost). Do not persist raw chain-of-thought (ADR-025).
- The migration path uses the real Debezium PostgreSQL connector into `legacy.trades.cdc`, normalized
  into the canonical Avro `TradeEvent`. Do not simulate CDC in application code (ADR-020).
- The 15-case golden dataset proves regression discipline and failure-mode coverage, not statistical
  accuracy. Do not write accuracy claims from it (ADR-024).

Implementation order is phased in `README.md`; deterministic streaming and security enforcement land
before any agent work.

## Path-scoped rules

`.claude/rules/*.md` auto-apply by file path and carry the detailed conventions. Consult them rather
than restating: `architecture.md` (layering, naming), `java-modern.md` (records, sealed types,
pattern matching, virtual threads, scoped values), `kafka.md` (idempotency, DLT naming, consumer
group naming), `testing.md` (JUnit 5 + Mockito + AssertJ, Testcontainers over H2, `@MockitoBean`),
`database.md`, `security.md`.

Two rules files still carry placeholders worth filling as the code lands: the Topics table in
`kafka.md` and the Architecture section in `architecture.md`. `database.md` and `security.md` were
copied from an unrelated GST invoicing project and their invoice-numbering and GSTIN sections do not
apply here.
