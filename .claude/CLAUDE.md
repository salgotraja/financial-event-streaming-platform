# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state: design-complete, implementation not started

`docs/` holds roughly 6,000 lines of specification describing about 20 services. The build is a
multi-module Gradle project whose only modules so far are `contracts` (empty, schemas land in Phase 1)
and `platform-common` (shared Testcontainers fixtures only).

There are no producers, consumers, entities, repositories, controllers, Avro schemas or Flyway
migrations. Read the actual source before assuming any component exists.

## Build and test

Gradle wrapper, not Maven. There is no `./mvnw` in this repo.

```bash
./gradlew build                                              # compile + test + plane isolation check
./gradlew test                                               # tests only
./gradlew checkPlaneIsolation                                # the central invariant, as a build rule
./gradlew :platform-common:test                              # single module
./gradlew test --tests 'dev.engnotes.fes.SomeTest'           # single class
```

- Toolchain: Java 25 (LTS, pinned in the root build), Gradle 9.5.1, Spring Boot 4.1.0.
- Root is an aggregator and holds no source. `bootRun` is per-service once services exist.
- `./gradlew test` requires a running Docker daemon.
- Shared container wiring is `dev.engnotes.fes.testing.TestcontainersConfiguration` in
  `platform-common` test fixtures. Services consume it with
  `testImplementation testFixtures(project(':platform-common'))`.
- No formatter and no linter are configured. `check` maps to test plus `checkPlaneIsolation`.

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

Modules land per README phase. Do not create empty modules ahead of the phase that needs them; add
`include 'services:<group>:<service>'` to `settings.gradle` when the work starts.

Each service owns its image, workload identity, consumer group, scaling policy and database schema.
`contracts` and `platform-common` are the only shared code and neither may hold business logic.

## Which docs are authoritative

`docs/requirements-v1.2.md`, `docs/architecture-v1.2.md`, `docs/specification-v1.2.md`, and the ADRs
in `docs/adr/` (ADR-001 through ADR-028, one file each).

Everything in `docs/archive/` is superseded. Do not implement from it and do not cite it.
`docs/archive/README.md` maps each archived file to its replacement.

Avro event schemas are inline in `docs/specification-v1.2.md` under namespace `dev.engnotes.fes.events`
and land as files in `contracts/src/main/avro/`. The Kafka topic inventory with retention and
partition counts is in `docs/architecture-v1.2.md` and mirrored in `.claude/rules/kafka.md`.

## Architecture: five planes and the isolation invariant

1. Ingestion and migration: synthetic simulators plus Debezium CDC from a mock legacy PostgreSQL.
2. Deterministic streaming: enrichment, risk evaluation, position and exposure read models. Targets
   50k events/sec and sub-200ms risk latency.
3. Audit and control: immutable S3 Object Lock evidence, signed manifests, reconciliation.
4. Candidate and case: deterministic anomaly screening, alert cases, maker-checker rule governance.
5. Agent investigation: bounded LLM investigation, typed read-only tools, Neo4j precedent graph,
   human review.

**The invariant that governs most design choices:** Plane 5 is asynchronous and optional. Provider
outage, throttling, graph unavailability, or budget exhaustion in the agent plane must never block or
add an availability dependency to Planes 1 through 3. The two planes carry separate SLOs; the
50k/sec figure is a deterministic-streaming target and never a claim about LLM throughput.

`./gradlew checkPlaneIsolation` enforces the build half of this: no ingestion, streaming or audit
module may depend on an agent module, on Neo4j, or on an LLM provider. The check currently inspects
zero modules and says so, because none of those modules exist yet.

## Binding invariants from the ADRs

- At-least-once delivery with idempotent producers, explicit offset management, and deterministic
  idempotency keys. Do not introduce Kafka transactions, do not set `isolation.level=read_committed`,
  and do not describe the system as exactly-once (ADR-019).
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
than restating: `architecture.md` (module layout, layering, naming), `java-modern.md` (records,
sealed types, pattern matching, virtual threads, scoped values), `kafka.md` (topic inventory,
idempotency, `.dlq` naming, consumer group naming), `database.md` (store ownership, Flyway per
module, idempotent writes), `security.md` (identity, agent tool boundary, audit integrity),
`testing.md` (Testcontainers, required test categories).

## Agent configuration is generated

`.claude/` is the single source. After editing anything under `.claude/skills/` or `.claude/agents/`,
run:

```bash
./scripts/sync-agent-config.sh          # regenerate .agents/skills and .codex/agents
./scripts/sync-agent-config.sh --check  # verify without writing, suitable as a CI gate
```

Never hand-edit `.agents/skills/` or `.codex/agents/`; those changes are overwritten.
