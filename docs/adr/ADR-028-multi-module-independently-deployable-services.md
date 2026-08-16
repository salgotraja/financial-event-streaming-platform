# ADR-028: Multi-Module Repository With Independently Deployable Services

**Status:** Accepted
**Date:** 2026-08-16

## Context
The architecture defines roughly 18 services across five planes, each with its own workload identity,
consumer group and scaling policy. The repository currently holds a single Spring Boot application.
Two decisions were open: how services are packaged, and where the code lives.

Packaging was not genuinely open. The identity-trust matrix assigns a distinct workload identity per
service, KEDA and Application Auto Scaling scale consumer groups independently, and FR-08 sets
different lag thresholds per service. A modular monolith cannot satisfy any of those.

## Decision
Every service is an independently deployable unit: its own container image, workload identity,
consumer group, scaling policy, database schema and release cadence.

Those services live in one repository as Gradle modules grouped by plane:

```text
contracts/              Avro schemas and generated types
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

The build enforces the Plane 5 isolation invariant: no module under `services/ingestion`,
`services/streaming` or `services/audit` may declare a compile or runtime dependency on any module
under `services/agent`, or on the Neo4j or LLM-provider libraries. A violation fails the build.

## Alternatives
- **Polyrepo, one repository per service.** Maximum deployment independence and the common enterprise
  shape. Rejected for this project: `TradeEvent` is shared by four services, so the Avro contracts
  need either a published artifact with its own release cycle or duplication. For a solo project that
  overhead buys nothing, and a cross-cutting change to the logging contract would touch 18 pull
  requests. The isolation invariant would also become unenforceable, since no build sees both sides.
- **Modular monolith, one deployable with internal module boundaries.** Simplest to build and run.
  Rejected: contradicts per-service identity, per-service scaling and the plane isolation invariant.
  A single deployable means an agent-plane failure shares a JVM with the deterministic hot path,
  which is exactly what ADR-026 forbids.
- **Multi-module repository, chosen.** Shared contracts without publishing overhead, one place to run
  the whole test suite, and the isolation rule mechanically enforceable because the build graph spans
  every plane.

## Consequences
The shared repository is a build-and-release convenience, never a shared runtime. Specifically:

- No two services share a database schema. Cross-service reads go through Kafka or a versioned HTTP
  contract.
- `contracts` and `platform-common` are the only shared code, and neither may hold business logic.
  Growth there is reviewed against that charter.
- Each service produces its own image and can be deployed alone. CI builds and publishes per module.
- Modules land per README phase rather than up front. Empty modules are not created in advance.

If the project later needs per-service release cadences that a single repository cannot express,
splitting out is a mechanical move because the module boundaries and the shared-code charter already
match service boundaries.
