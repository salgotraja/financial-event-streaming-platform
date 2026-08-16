# Financial Event Streaming Platform — v1.2

**Security-first financial streaming + migration + controlled agentic investigation**

## What v1.2 demonstrates

1. **Deterministic financial streaming** — Kafka/Avro, enrichment, risk, position/exposure, immutable audit, DLQ/replay, 50k events/sec target.
2. **Enterprise security/IAM** — workload identity, OIDC human identity, least privilege, MSK IAM, policy enforcement, KMS signing, Object Lock, negative security tests.
3. **Financial control/governance** — maker/checker rule changes, alert cases, reconciliation/control evidence.
4. **Legacy modernization** — PostgreSQL + Debezium CDC -> canonical Kafka event stream, coexistence/backfill/cutover evidence.
5. **AI investigation** — bounded agent over anomaly candidates, read-only tools, human review, Neo4j precedent graph.
6. **AI reliability** — versioned golden/adversarial datasets, deterministic assertions, calibrated judge only where required, CI regression gate.
7. **Architecture honesty** — no claim that the LLM handles 50k events/sec, no claim that 15 examples prove production accuracy, no claim that Neo4j is required by scale, and no real money movement.

## Recommended implementation order

### Phase 0 — Identity, trust and threat model
Commit the identity/trust matrix, threat model and policy boundaries first.

### Phase 1 — Core event spine
Kafka/Schema Registry + Trade/Market/Corporate producers + audit skeleton.

### Phase 2 — Market-data projection, enrichment, deterministic risk, position/exposure
Prove event-fed cache freshness, per-record poison handling, correctness, dependency failure behavior and observability before AI.

### Phase 3 — Security enforcement
MSK IAM, per-workload roles, locked/signed audit, human OIDC control plane, negative authorization tests.

### Phase 4 — CDC migration
Real Debezium snapshot/change capture + normalizer + dedup/cutover report.

### Phase 5 — Reconciliation/candidate layer
Reconciliation observations + deterministic anomaly candidates.

### Phase 6 — Agent investigation
Typed tool gateway + human review + decision trace.

### Phase 7 — Graph precedent + eval
Neo4j projection, golden/adversarial regression suite, CI gate.

### Phase 8 — Performance/failure evidence
50k deterministic load test, broker/connector failures, agent/provider outage isolation, cost report.

## Start here

- `requirements-v1.2.md`
- `architecture-v1.2.md`
- `specification-v1.2.md`
- `docs/security/identity-trust-matrix.md`
- `docs/adr/ADR-021-deterministic-before-agent.md`
- `docs/evals/golden-dataset-design-notes-v1.1.md`

## Additional design documents

- `docs/reconciliation-agent-v1.2.md`
- `docs/portfolio-positioning-v1.2.md`
- `docs/adr/ADR-022-graph-layer-detailed.md`
- `docs/evals/evaluation-regression-harness-v1.2.md`
