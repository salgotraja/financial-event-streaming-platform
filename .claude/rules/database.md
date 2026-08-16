---
description: PostgreSQL, Flyway and Neo4j rules for state, governance and precedent
paths: "**/repository/**,**/entity/**,**/domain/**,db/migration/**,**/db/migration/**,**/*Repository.java,**/*Entity.java"
---

## Store Ownership

PostgreSQL is authoritative for risk state, the position and exposure read model, alert cases, risk-rule
versions, agent decisions and human-review verdicts (ADR-008).

Neo4j holds a **derived, rebuildable** precedent graph. It is never a source of truth. Code must degrade
to a flat reviewed-case lookup or an explicit "precedent unavailable" state when Neo4j is down, and must
never block deterministic processing on it (ADR-022).

Redis holds projected market and instrument state only. It is a cache, never a store of record.
Population is event-driven from `market-data.ticks`; on a miss, emit `REFERENCE_DATA_UNAVAILABLE` and
take the retry/DLQ path. Never fall back to a synchronous simulator call (ADR-027).

**Each service owns its own schema. No two services share tables.** Cross-service reads go through
Kafka or a versioned HTTP contract, never a shared table (ADR-028).

## Migration Rules

- Tool: Flyway, PostgreSQL dialect.
- Location: `src/main/resources/db/migration/` inside the owning service module.
- Naming: `V{version}__{Description_with_underscores}.sql`.
- Versions are per module. Two services may both have a `V1__`; they are separate schemas.
- NEVER modify an existing migration. Always create a new one.
- Every migration carries a header comment stating what it changes and why.

## Idempotency and Reprocessing

Delivery is at-least-once (ADR-019), so every write path that consumes Kafka must be idempotent:

- Position updates are idempotent by `tradeId`; reprocessing must not double-count (FR-11.3).
- The migration normalizer deduplicates on
  `hash(sourceSystem + sourceRecordKey + sourceChangePosition + canonicalEventType)`.
- The precedent graph projection deduplicates on `reviewId` and checkpoints the consumed offset.
- Prefer a unique constraint plus `ON CONFLICT DO NOTHING` over a read-then-write check, which races.

## Entity Rules

- Records are the default for data carriers, but not for JPA entities. This project uses Spring Data
  JDBC, so aggregate roots may be records where the mapping allows it.
- `@Column(nullable = false)` stated explicitly. Do not rely on inference.
- Optimistic locking on any row carrying a running total, notably risk position state (ADR-008).
- Audit and governance rows are append-only. No update, no delete, no soft-delete flag. A correction
  is a new row plus a new lifecycle event (ADR-016).
- Rebuildable read models must support a full rebuild from Kafka history and report a reconciliation
  result afterwards (FR-11.5).

## Query Rules

- Always paginate with `Pageable`. Never fetch an unbounded collection.
- Case timelines and position lookups are indexed point queries, not scans.
- Do not put analytical queries against operational stores. Historical analysis goes to the Parquet
  audit archive through Athena (ADR-005).

## Classification

`traderId`, `accountId`, alert investigations, risk rules, audit evidence and security events are
treated as `RESTRICTED` even though all data is synthetic (NFR-10.2). Classification drives access
policy, log redaction, key choice, retention and export permission.
