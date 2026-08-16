---
description: Module layout, layering and naming for backend changes
paths: "**/*.java,settings.gradle,**/build.gradle"
---

## Module Layout

Independently deployable services as Gradle modules, grouped by plane (ADR-028):

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

Package root is `dev.engnotes.fes.{service}`. Modules land per README phase; do not create empty
modules ahead of the phase that needs them.

## Hard Constraints

- **Plane isolation.** No module under `services/ingestion`, `services/streaming` or `services/audit`
  may depend on any module under `services/agent`, or on Neo4j or LLM-provider libraries. The build
  fails on violation. This is the project's central invariant (ADR-026, ADR-028).
- **Shared code is `contracts` and `platform-common` only**, and neither holds business logic. If a
  change wants to add domain logic there, it belongs in a service.
- **No shared database schemas.** Cross-service reads go through Kafka or a versioned HTTP contract.
- Each service has its own image, workload identity, consumer group and scaling policy.

## Layer Rules

- Controller: validates input, maps to and from DTOs, delegates to a service. No business logic.
- Consumer: deserialises, delegates to a service, manages acknowledgement and the DLQ path. No
  business logic.
- Service: business logic only. No HTTP concerns, no persistence concerns.
- Repository: persistence only. No business logic.
- No `@Transactional` on controllers or consumers. Services own transaction boundaries.

## Naming Conventions

- DTOs: `{Entity}Request`, `{Entity}Response`
- Services: `{Domain}Service`
- Repositories: `{Entity}Repository`
- Kafka consumers: `{Topic}Consumer`; producers: `{Topic}Producer`
- Avro events: `{Thing}Event` in `dev.engnotes.fes.events`
- Consumer group name equals the service name, no suffix. See `kafka.md`.

## Two SLO Domains

Deterministic plane: 50,000 events/sec, sub-200ms p99 end to end, sub-10ms p99 enrichment, sub-5ms
p99 risk evaluation. Agent plane: measured separately in candidates/sec and time-to-case.

Never attribute a deterministic-plane number to the agent plane. The agent may lag, throttle, shed
work or be entirely absent without affecting deterministic risk processing.

## Authoritative Documents

`docs/requirements-v1.2.md`, `docs/architecture-v1.2.md`, `docs/specification-v1.2.md`, and the ADRs
under `docs/adr/`. Anything under `docs/archive/` is superseded and must not be implemented from.
