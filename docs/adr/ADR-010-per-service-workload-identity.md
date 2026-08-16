# ADR-010: Per-Service ECS Task Roles and EKS Pod Identity

**Status:** Accepted
**Date:** 2026-08-16

## Context
A shared service role makes least privilege unprovable: any workload can do what the most privileged
workload can do, and a compromise has the blast radius of the whole platform.

## Decision
Every independently deployable service receives its own IAM role: a dedicated `taskRoleArn` on ECS,
and a dedicated Kubernetes service account mapped to EKS Pod Identity on EKS. The ECS execution role
is shared and minimal, covering only ECR pull and log shipping, and grants no application data access.

## Alternatives
- **One platform role for all services.** Simplest IaC. Rejected: defeats NFR-05.3 and makes the
  negative test matrix meaningless.
- **IRSA instead of EKS Pod Identity.** Established and widely documented. Rejected: Pod Identity
  removes the OIDC-provider-per-cluster setup and is the current AWS direction; the application code
  is identical either way.

## Consequences
Roughly 18 roles and policies to maintain in IaC, which is the point: each is small enough to read.
Every new service must add an identity-trust-matrix entry and at least one ALLOW and two DENY tests
before it is considered complete.
