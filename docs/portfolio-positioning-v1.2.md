# Portfolio & Consulting Positioning — v1.2

## Honest one-line description

**Security-first financial event streaming and control platform with tested legacy CDC migration, workload identity, immutable audit, and a human-gated AI investigation layer.**

## What the project proves

### BFSI / Principal Architecture
- high-throughput event architecture;
- schema governance and migration;
- risk/control workflows and maker-checker separation;
- workload/human identity and least privilege;
- immutable/tamper-evident audit;
- observability, failure engineering and cost trade-offs.

### IAM / Identity Architecture
- distinct human, workload and agent identities;
- AWS workload identity instead of long-lived keys;
- fine-grained topic/resource authorization;
- externalized privileged policy;
- agent tool authorization enforced outside the model;
- separation of duties and evidence.

### AI Architecture
- deterministic screening before LLM use;
- bounded tool-calling agent;
- human approval boundary;
- graph precedent retrieval;
- structured decision evidence;
- golden/adversarial regression suites;
- measured cost/latency.

### Modernization Consulting
- legacy PostgreSQL -> Debezium CDC -> canonical Kafka stream;
- snapshot + CDC coexistence;
- schema mapping;
- deduplication and restart;
- reconciliation evidence;
- cutover/rollback thinking.

## Claims to avoid
Do not say:
- "production compliant";
- "the AI handles 50k events/sec";
- "15 cases prove 95% model accuracy";
- "Neo4j was needed for scale";
- "exactly once" without defining application semantics;
- "the agent autonomously fixes financial anomalies."

## Strong interview phrasing

> I designed the system as two reliability domains: a deterministic high-throughput financial-control plane and a bounded AI investigation plane. The AI can fail without affecting risk processing. It receives only anomaly candidates, uses capability-scoped tools, produces auditable structured decisions, and cannot cross the human-approval boundary. I also built the legacy migration path with real CDC rather than simulating it, and I validate security and AI reliability through negative tests and regression gates.

## Consulting offers the artifact can support

1. Kafka/event-platform production-readiness review.
2. Legacy batch-to-streaming migration architecture.
3. Event-driven risk/reconciliation control design.
4. Workload-identity and least-privilege review for streaming platforms.
5. AI-assisted anomaly-investigation architecture with human safety gates.
6. Observability/load/failure validation of Kafka consumer platforms.

The project demonstrates capability; any client engagement must still begin with discovery and domain-specific regulatory/control requirements.
