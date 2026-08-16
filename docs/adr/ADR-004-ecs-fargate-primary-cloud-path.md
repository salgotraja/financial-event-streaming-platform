# ADR-004: ECS Fargate as the Primary AWS Container Validation Path

**Status:** Accepted
**Date:** 2026-08-16

## Context
The project validates on AWS in short-lived, cost-capped runs. Two container platforms are in scope:
ECS Fargate and EKS. Maintaining both as primary would double the IaC, identity and observability work.

## Decision
ECS Fargate is the primary validation path. EKS is a secondary profile carrying the Kubernetes-specific
material: KEDA, NetworkPolicies, Pod Identity and the SPIFFE/SPIRE advanced-security experiment.

## Alternatives
- **EKS as primary.** Richer ecosystem and the SPIFFE story lives there. Rejected: control-plane cost
  runs continuously and node management adds teardown risk to an ephemeral budget-capped lab.
- **Both as co-primary.** Rejected: duplicated effort with no additional architectural evidence.

## Consequences
Two autoscaling mechanisms must be maintained (ADR-003 and ADR-015) and two workload-identity
mechanisms (ADR-010). Both are deliberate: showing that the application code is unchanged across them
is part of the demonstration.
