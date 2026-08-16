# ADR-018: Security Telemetry Is First-Class Observability

**Status:** Accepted
**Date:** 2026-08-16

## Context
Authentication failures, authorization denials and privileged actions are usually buried in
application logs, discovered only during incident review. NFR-08.4 requires detecting a simulated
security-control failure in under two minutes.

## Decision
Emit security decisions as structured metrics and as `security.events` records alongside normal
telemetry, with a dedicated Grafana dashboard and alert rules. Signals carry actor and workload
identity, target resource, action, decision, reason and correlation ID, and never carry credentials,
tokens or key material.

## Alternatives
- **Rely on CloudTrail.** Rejected: covers AWS control-plane calls only. It sees nothing of
  maker-checker denials, agent tool authorization or audit signature verification.
- **Log-only, queried on demand in Loki.** Rejected: no alerting path, so mean time to detect is
  measured in whenever someone looks.

## Consequences
Every service that makes an authorization decision has a telemetry obligation, enforced by the
per-service completion gate. CloudTrail augments rather than replaces application decision logging.
Emitting these signals must not leak the identifiers NFR-05.16 restricts.
