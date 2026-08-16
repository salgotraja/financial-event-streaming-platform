# ADR-027: Per-Record Poison Quarantine and Event-Fed Market Cache

**Status:** Accepted  
**Date:** 2026-08-16

## Decisions
1. Poison records are quarantined individually after bounded retry; they never open an event-type-wide circuit breaker.
2. Circuit breakers protect calls to failing dependencies, not schema/event categories.
3. Redis market state is projected from `market-data.ticks`. Trade enrichment does not synchronously call the simulator as a cache-miss fallback.

## Why
These boundaries prevent one malformed event from causing a broad outage and prevent a cache miss from introducing a synchronous simulator dependency into the high-throughput enrichment path.
