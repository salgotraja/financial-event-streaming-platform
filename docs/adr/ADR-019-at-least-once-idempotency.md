# ADR-019: At-Least-Once Processing with Idempotency and Deduplication

**Status:** Accepted  
**Date:** 2026-08-16

## Context
The platform is append-oriented and already tolerates duplicate audit records while prohibiting event loss. Blanket Kafka transactions would add operational and latency complexity to paths that do not require atomic multi-topic writes.

## Decision
Use at-least-once delivery, idempotent Kafka producers, explicit consumer offset management, and deterministic event/idempotency keys. Use transactions only for a future path that demonstrates a concrete atomicity requirement.

## Consequences
The project must prove deduplication/restart behavior. "Exactly once" is not used as a marketing claim where the application semantics are still at-least-once.
