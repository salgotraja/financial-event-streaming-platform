# ADR-011: Human OIDC Identity With an Externalized Policy Decision Point

**Status:** Accepted
**Date:** 2026-08-16

## Context
Privileged operations (rule approval, DLQ replay, case disposition, audit export) need authenticated
human identity and rules that are more than role checks: maker-checker requires comparing the actor
against the proposer, and case closure depends on severity.

## Decision
Authenticate humans through an OIDC-compatible IdP (Keycloak locally, enterprise IdP in cloud). The
Administrative Control Plane is the Policy Enforcement Point; an externalized engine (OPA/Rego) is the
Policy Decision Point evaluating subject, roles, action, resource state, environment and stated reason.

## Alternatives
- **Spring Security `@PreAuthorize` expressions only.** No extra component. Rejected: separation-of-duties
  rules become scattered annotations that cannot be tested, versioned or audited as a policy artifact.
- **Custom in-service authorization service.** Rejected: reimplements a solved problem and produces no
  policy language reviewers can read.

## Consequences
Policy is a versioned, testable artifact separate from application code, and a decision can be
replayed against its inputs. Every decision, ALLOW and DENY, emits a `SecurityEvent`. The PDP is on
the path of privileged operations only, never on the streaming hot path.
