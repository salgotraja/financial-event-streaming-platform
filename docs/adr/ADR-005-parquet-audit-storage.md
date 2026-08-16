# ADR-005: Parquet for the Immutable Audit Archive

**Status:** Accepted
**Date:** 2026-08-16

## Context
FR-05 requires every event archived to S3, queryable through Athena within five minutes, immutable,
and cheap enough to retain. Compliance queries are analytical: counts by topic and offset range,
filters by date and event type.

## Decision
Write batched Parquet objects partitioned by `year/month/day/event_type`, each with a sidecar
manifest (ADR-012).

## Alternatives
- **Raw Avro or JSON per event.** Simplest writer. Rejected: object-per-event destroys S3 request
  economics at 50,000 events/sec and makes Athena scans prohibitively expensive.
- **JSON Lines batches.** Simple and greppable. Rejected: no column pruning, roughly four to ten
  times the storage, and full scans on every reconciliation query.

## Consequences
The Audit Service must buffer and flush in bounded batches, so archive visibility is delayed by the
flush interval rather than immediate. This is the mechanism behind the FR-05.5 five-minute target.
Column pruning makes the FR-14 reconciliation queries affordable.
