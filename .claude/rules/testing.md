---
description: Test conventions and available infrastructure
paths: "**/src/test/**/*.java"
---

## Framework

- Unit tests: JUnit 5 + Mockito, no Spring context, pure logic.
- Integration tests: `@SpringBootTest` or test slices (`@WebMvcTest`, `@DataJdbcTest`).
- Kafka: **Testcontainers** (`apache/kafka-native`), not `@EmbeddedKafka`. Tests run against the same
  broker implementation as the deployed system, for the same reason Testcontainers PostgreSQL is used
  instead of H2.
- Database: Testcontainers PostgreSQL. Never H2 as a production substitute.
- Shared container wiring lives in `TestcontainersConfiguration`. `./gradlew test` requires a running
  Docker daemon.

## Rules

- No PowerMock.
- Use `@MockitoBean`, not the removed `@MockBean`.
- Build test data with builders or record factory methods, never long raw constructor calls.
- Name pattern: `should_{expectedBehaviour}_when_{condition}`.
- Use AssertJ `assertThat`, not JUnit `assertEquals`.
- Pin container image tags to the version being targeted rather than `:latest`, so a test run is
  reproducible and an upstream release cannot break the build overnight.

## Test Slice Guide

- `@WebMvcTest`: controller validation, HTTP mapping, auth filters.
- `@DataJdbcTest`: repository queries, migrations, constraints.
- `@SpringBootTest`: full integration paths, Kafka end to end.
- Plain JUnit: service logic, domain objects, pure functions.

## Required Coverage by Category

These follow from the requirements and are not optional for a module to be complete:

- **Idempotency.** Every Kafka consumer has a test that processes the same event twice and asserts a
  single effect. Delivery is at-least-once (ADR-019).
- **Poison record.** A malformed record is quarantined to `{topic}.dlq`, the offset advances, and the
  next record on the same partition processes normally (ADR-027).
- **Dependency failure.** A Redis or PostgreSQL outage opens the circuit at the dependency boundary
  and does not quarantine otherwise valid records.
- **Negative authorization.** At least one ALLOW and two DENY tests per service. A `RiskMaker`
  approving a rule they proposed must be denied (ADR-016).
- **Agent tool boundary.** Unknown tool name denied, undeclared argument rejected, instruction-like
  text injected into event or precedent fields does not change tool behaviour, and a failed required
  tool produces `ESCALATE` rather than `NO_FLAG` (ADR-023).
- **Plane isolation.** A build-level check asserting no ingestion, streaming or audit module depends
  on an agent module (ADR-028).
- **Rebuild.** Read models and the precedent graph rebuild from event history and report a
  reconciliation result (FR-11.5, FR-22.3).

## Evaluation Suite

Agent evaluation is separate from unit and integration tests. Deterministic assertions come first;
an LLM judge is used only for narrative and evidence quality, and only once calibrated against human
scoring. The 15-case golden set is a regression suite, not an accuracy benchmark. Never write an
accuracy percentage from it (ADR-024).
