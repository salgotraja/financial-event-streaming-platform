# ADR-021: Deterministic Candidate Screening Before LLM Investigation

**Status:** Accepted  
**Date:** 2026-08-16

## Context
The deterministic platform targets 50,000 events/sec. Invoking an LLM per event would destroy the project's latency/cost model and make core risk processing dependent on an external model provider.

## Decision
Keep the hot path deterministic. A candidate service promotes a bounded subset of events/observations to `anomaly.candidates`; the agent investigates asynchronously.

## Consequences
The agent can lag, throttle or fail without blocking deterministic risk/audit. Agent throughput and cost are measured separately.
