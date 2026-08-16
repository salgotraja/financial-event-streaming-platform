# ADR-016: Risk Rules Are Versioned Entities Under Maker-Checker Governance

**Status:** Accepted
**Date:** 2026-08-16

## Context
Risk thresholds decide whether a breach is detected. An operator who can silently widen a threshold
can disable detection, and a configuration file gives no record of who changed what, when or why.

## Decision
Model rules as versioned entities with lifecycle states `DRAFT`, `PENDING_APPROVAL`, `ACTIVE`,
`RETIRED`, `REJECTED`. A material change requires a RiskMaker to propose and a different RiskChecker
to approve. Every transition emits an immutable `risk-rules.events` record carrying both identities,
reason and correlation ID. Rollback creates a new lifecycle event; history is never rewritten.

## Alternatives
- **YAML in a ConfigMap with Git review.** Cheap. Rejected: Git approval is not runtime-enforced, and
  an operator with cluster access bypasses it entirely.
- **Database rows with an audit trigger.** Rejected: no state machine, no separation of duties, and
  the active rule set cannot be reconstructed from an event history.

## Consequences
The Risk Alert Service derives its active rule set from `risk-rules.events` and honours
`effectiveAt`, so rule activation is observable and replayable. Self-approval is denied at the policy
layer and covered by a negative test, not left to UI convention.
