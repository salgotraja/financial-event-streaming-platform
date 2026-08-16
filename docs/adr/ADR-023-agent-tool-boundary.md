# ADR-023: Agent Tools Are Capability-Bounded and Human-Gated

**Status:** Accepted  
**Date:** 2026-08-16

## Decision
The model has read-only investigation tools plus `flag-for-review`, which can only create a proposal. It has no credential for ledger mutation, risk-rule approval, DLQ replay, IAM administration, audit retention changes, or remediation execution.

Human reviewers authenticate separately and any approved "remediation" in v1.2 emits only a synthetic intent/case transition.

## Rationale
Safety is an authorization property enforced outside the model, not a behavioral instruction asking the model to be careful.
