# ADR-017: Position and Exposure as an Event-Driven Read Model

**Status:** Accepted
**Date:** 2026-08-16

## Context
Risk analysts need current net position, gross buy and sell quantities and market value by account,
trader and ticker, without those queries competing with the enrichment and risk hot path.

## Decision
Build a CQRS-style read model in a dedicated service consuming `trades.enriched`, keyed and made
idempotent by `tradeId`, publishing `positions.snapshots` and exposing a read-only API. The model must
be rebuildable in full from Kafka history and must report a reconciliation result after rebuild.

## Alternatives
- **Query the risk service's position state directly.** One less service. Rejected: couples an
  analyst-facing API to the latency-critical risk path and shares a schema across services.
- **Compute positions on demand from the audit archive.** Rejected: Athena latency is minutes; the
  requirement is a real-time operational view.

## Consequences
Duplicated position state exists in the risk service and the read model, and the two can diverge; the
rebuild-and-reconcile path (FR-11.5) is how divergence is detected. Idempotency by `tradeId` is
mandatory because delivery is at-least-once (ADR-019). This is a read model, not a settlement ledger.
