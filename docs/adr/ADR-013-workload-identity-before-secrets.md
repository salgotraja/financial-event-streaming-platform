# ADR-013: Workload Identity First, Secrets Manager as Fallback

**Status:** Accepted
**Date:** 2026-08-16

## Context
Every stored secret is a rotation obligation, a leak surface and an audit finding. Some targets still
require a shared credential.

## Decision
Apply a strict preference order: temporary workload credentials first; short-lived generated
credentials such as IAM database authentication second; automatically rotated Secrets Manager entries
only when a target genuinely requires a shared secret. Endpoints and bootstrap addresses are
configuration, not secrets, and are not placed in Secrets Manager to obscure them.

## Alternatives
- **Secrets Manager for everything uniformly.** Consistent. Rejected: converts problems that workload
  identity already solves into rotation and access-control problems.
- **Environment variables from CI.** Rejected: NFR-05.10 prohibits plaintext secrets in task
  definitions, manifests, CI logs and source control.

## Consequences
Most services carry an empty `secrets` block in their task definition, which is the intended evidence.
Adding a secret requires justifying why workload identity cannot cover the case.
