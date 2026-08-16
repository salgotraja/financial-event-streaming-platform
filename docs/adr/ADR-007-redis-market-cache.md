# ADR-007: Redis for the Market and Reference Data Cache

**Status:** Accepted
**Date:** 2026-08-16

## Context
NFR-01.2 requires p99 enrichment below 10ms at 10,000 events/sec. Every enriched trade needs current
market state and instrument reference data.

## Decision
Keep latest market state and instrument reference data in Redis, populated by the market-data cache
projector consuming `market-data.ticks` (ADR-027).

## Alternatives
- **PostgreSQL lookup per trade.** One less component. Rejected: single-digit-millisecond p99 under
  10,000 lookups/sec is not achievable without effectively rebuilding a cache.
- **In-process JVM cache per consumer.** Lowest latency and no network hop. Rejected: 12 consumer
  instances each hold an independent view, so freshness and cache-miss behaviour become
  instance-dependent and untestable. Retained as a second-level cache option in front of Redis.

## Consequences
Redis is a hot-path dependency and needs a circuit breaker at the dependency boundary. Cache miss is
an explicit, measured failure path emitting `REFERENCE_DATA_UNAVAILABLE`, never a synchronous
fallback to the simulator (ADR-027).
