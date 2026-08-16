# ADR-015: ECS Application Auto Scaling Instead of KEDA on ECS

**Status:** Accepted
**Date:** 2026-08-16

## Context
KEDA is the EKS scaling mechanism (ADR-003). ECS is the primary cloud path (ADR-004) and KEDA is a
Kubernetes controller with no ECS equivalent.

## Decision
On ECS, export a lag-derived CloudWatch metric from each consumer and drive desired task count with
Application Auto Scaling target-tracking or step-scaling policies. Lag is the primary demand signal;
CPU and memory policies may coexist as guard rails.

## Alternatives
- **Run KEDA in a sidecar Kubernetes cluster to control ECS.** Rejected: absurd coupling for the benefit.
- **CPU-based ECS scaling only.** Rejected: same defect as CPU-based HPA, uncorrelated with lag.
- **Custom Lambda scaler.** Rejected: reimplements target tracking with worse observability.

## Consequences
Two scaling implementations exist, one per platform, with identical application code. Metric math and
cooldown values are load-test outputs and are committed as CDK configuration rather than guessed.
