# ADR-008: PostgreSQL for Risk, Position, Case and Governance State

**Status:** Accepted
**Date:** 2026-08-16

## Context
Risk rule evaluation carries running per-trader position totals. Cases, rule versions, review
decisions and reviewer verdicts are governed records where a lost or reordered write is a control
failure.

## Decision
Use PostgreSQL with optimistic locking for risk state, the position and exposure read model, alert
cases, risk-rule versions and human-review records. PostgreSQL is authoritative for all case,
decision and feedback state.

## Alternatives
- **DynamoDB.** Scales without operational effort. Rejected: the maker-checker and case-transition
  workflows need multi-row transactional integrity and relational queries for case timelines.
- **Kafka topics as the only store, with state rebuilt on demand.** Rejected: the FR-13.5 case
  timeline and the FR-11.4 position API need indexed point queries, not a log scan.

## Consequences
Each service owns its own schema and no two services share tables; cross-service reads go through
Kafka or a versioned API. Position state must remain idempotent by `tradeId` (FR-11.3) because
delivery is at-least-once (ADR-019). Neo4j never becomes authoritative (ADR-022).
