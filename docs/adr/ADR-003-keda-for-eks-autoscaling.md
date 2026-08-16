# ADR-003: KEDA for EKS Kafka Consumer Autoscaling

**Status:** Accepted
**Date:** 2026-08-16

## Context
Kafka consumer demand is expressed as partition lag, not CPU. Scaling on CPU reacts late and scales
the wrong direction when consumers are blocked on I/O rather than compute.

## Decision
On EKS, use KEDA with a Prometheus scaler reading `kafka_consumer_lag_by_partition` to scale consumer
Deployments.

## Alternatives
- **HPA on CPU or memory.** Built in, no add-on. Rejected: no correlation between lag and CPU for
  I/O-bound consumers.
- **HPA with custom metrics via the Prometheus Adapter.** Works, and avoids an add-on. Rejected: KEDA
  provides scale-to-activation thresholds, per-trigger cooldown and a scaler catalogue with less
  wiring, and is the recognised Kubernetes answer for this problem.

## Consequences
KEDA is a cluster dependency and the metric must be accurate and available or scaling silently stops.
Maximum replicas are capped at the partition count (12), because replicas beyond that idle.
KEDA applies to EKS and local Kubernetes only; ECS uses ADR-015.
