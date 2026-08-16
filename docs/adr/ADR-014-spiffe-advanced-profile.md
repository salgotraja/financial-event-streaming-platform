# ADR-014: SPIFFE/SPIRE as an Advanced Service-Identity Profile, Not a Replacement for IAM

**Status:** Accepted
**Date:** 2026-08-16

## Context
AWS workload identity authorizes calls to AWS APIs. It does not authenticate service-to-service calls
between pods. The project also has a stated interest in attested workload identity as a learning goal.

## Decision
Deploy SPIRE in the EKS advanced-security profile and issue SVIDs to at least the trade-enrichment,
risk-alert and audit workloads for mTLS experiments. SPIFFE identity complements AWS IAM and never
replaces it for AWS resource authorization.

## Alternatives
- **Service mesh mTLS (Istio, Linkerd).** Rejected: a mesh brings a large operational surface for a
  property the project only needs to demonstrate on three workloads.
- **Skip service-to-service identity entirely.** Rejected: leaves a stated trust boundary in the
  threat model with no control behind it.

## Consequences
SPIFFE applies to the EKS profile only; ECS and local runs are unaffected. The learning motivation is
stated openly rather than presented as a scaling or compliance necessity. Required evidence includes
a negative test: a workload presenting the wrong SPIFFE ID is rejected by its peer.
