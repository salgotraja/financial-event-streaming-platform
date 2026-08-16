# ADR-025: Persist Structured Decision/Evidence Traces, Not Hidden Chain-of-Thought

**Status:** Accepted  
**Date:** 2026-08-16

## Decision
Audit model/provider/prompt/tool versions, source evidence, tool calls/results, precedent identifiers, typed draft/final decisions, critique outcome, reviewer verdict, latency, tokens and cost.

Do not require or persist raw hidden chain-of-thought.

## Rationale
The evidence needed to audit a system decision is the observable input/tool/output/control chain. Raw internal reasoning is unnecessary for the platform's control objectives and creates additional privacy/security/retention concerns.
