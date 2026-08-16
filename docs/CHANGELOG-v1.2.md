# v1.2 Change Log

## Preserved
All v1.1 business/security scope: financial event generation, enrichment, risk rules, positions/exposure, rule governance, cases, immutable/tamper-evident audit, DLQ/replay, observability, ECS/EKS validation, workload/human identity, security telemetry and supply-chain controls.

## Added
- real Debezium/PostgreSQL CDC migration path;
- explicit reconciliation-observation event stream;
- deterministic anomaly candidate service;
- bounded asynchronous agent investigation;
- typed agent outcomes and decision/evidence trace;
- human-review feedback events;
- derived Neo4j precedent graph;
- normalized 15-case golden regression dataset;
- adversarial AI-security cases;
- category-aware CI evaluation gates;
- AI cost/latency/queue metrics;
- independent deterministic vs agentic SLOs.

## Corrected / refined
- Agent no longer consumes every enriched event.
- "Reasoning logs" changed to structured evidence/tool/decision audit; raw hidden chain-of-thought is not required.
- Human approval cannot cause a real ledger correction in this portfolio; only synthetic remediation intent/case transition exists.
- 15-case dataset is described as regression coverage, not a production accuracy benchmark.
- Reconciliation/settlement-style golden cases now map to a real `ReconciliationObservationEvent`.
- Neo4j is explicitly a derived/rebuildable learning choice, not a scale necessity.
- Self-critique is bounded and triggered by risk/uncertainty plus high-confidence sampling, not only low confidence.
