# Financial Event Streaming Platform — Requirements & Positioning (v2)

## Why this project, and why this framing
Kafka expertise is in real, current demand in fintech specifically — not
generic backend work. The gap that actually commands a premium isn't
"knows Kafka," it's production-readiness in a regulated environment:
partition/retention decisions made deliberately, a tested recovery path,
schema governance enforced in CI, and a cost model that can be defended
stage-by-stage rather than assumed. Most Kafka deployments never validate
that story. This project is built specifically to make that story real
and demonstrable — with actual numbers from an actual load test — not to
claim it.

The most consistently-cited real-world pattern in the research is legacy
batch-to-streaming migration: banks and fintechs modernizing
nightly-batch/mainframe-era systems into event-driven architecture via a
strangler pattern, so the legacy system keeps running throughout. This
project's design should make that story tellable even though the
simulator itself generates market-data events, not migrated legacy data —
the architecture lessons (schema evolution, dual-write/backfill safety,
zero-downtime cutover) are the transferable part.

## Dual purpose — no conflict between them
1. **BFSI Principal Architect / VP Engineering interview artifact** — the
   original purpose. Answers the standard hard questions about scaling,
   tracing, observability, deployment, and failure handling with a real
   system, not a description of one.
2. **Case study for [[fintech-consulting-portfolio]]** — the same system,
   the same numbers, positioned for a consulting audience instead of an
   interview panel. No rework needed between the two; only the framing of
   the write-up changes.

## Niche consulting offers this project supports
Grounded in what actually shows demand, ranked by how directly this
project proves it:
1. **Legacy-to-streaming migration audits/design** — the highest-demand
   pattern found. "I'll assess your batch/legacy system and design a
   strangler-pattern migration to Kafka-based event streaming."
2. **Production-readiness review for existing Kafka deployments** —
   partition strategy, schema governance, disaster recovery, cost
   modeling. Sells to teams who already have Kafka but never validated it
   under regulatory/compliance scrutiny.
3. **KEDA-based autoscaling & cost optimization** — directly proven by
   this project's own load test. Sells to teams paying for
   over-provisioned consumers.
4. **Real-time fraud/reconciliation pipeline design** — a recurring
   demand pattern (payments, card settlement, risk scoring) adjacent to
   what this project already builds.
5. **(Forward-looking, not v1 scope)** Kafka as the backbone connecting
   agentic AI/LLM workflows to regulated fintech systems — a newer,
   less-crowded framing worth a mention once #1-4 are proven, not before.

## Functional Requirements (v1)

| ID | Requirement |
|----|-------------|
| FR-1 | `market-data-simulator` service produces realistic tick data via a Geometric Brownian Motion model, configurable per ticker |
| FR-2 | Same service has a load-test mode generating maximum sustained throughput for a configurable duration |
| FR-3 | Kafka (KRaft) cluster with Schema Registry; all event types defined as Avro schemas with compatibility checks enforced in CI |
| FR-4 | Consumer service(s) that enrich/process events, scaled by KEDA on consumer lag |
| FR-5 | OpenTelemetry tracing end-to-end (producer -> broker -> consumer), Prometheus metrics, Grafana dashboards |
| FR-6 | Dead letter topics, retry policies, circuit breakers, and poison-pill detection on the consumer side |
| FR-7 | Deployment: local (dev), ECS Fargate + Amazon MSK (production validation), EKS manifests (documented, validated once, then torn down) |
| FR-8 | k6 load test script executing the 5-phase test (calm / ramp / spike / recover / drain) against the pipeline |

## Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-1 (Throughput) | Sustain 50,000 events/sec at peak in the load test, with p99 processing latency measured and recorded, not estimated |
| NFR-2 (Scaling behavior) | Consumer lag must stabilize within a defined window (target: under 90 seconds) after a load spike, with KEDA scaling observed and screenshotted from Grafana |
| NFR-3 (Cost) | Cost per stage (ingestion, enrichment, storage) tracked and defensible — this is the differentiator identified in research, not an afterthought |
| NFR-4 (Schema governance) | No schema change reaches a topic without passing a compatibility check in CI — this is the "architectural, not operational" discipline the research flags as the real gap in most deployments |
| NFR-5 (Durability/DR) | Explicit replication factor and `min.insync.replicas` decision, documented with the reasoning, plus a tested (not just designed) recovery path for a broker failure |
| NFR-6 (Observability) | Every event traceable end-to-end via a correlation ID; trace data queryable, not just logged |

## Interview & consulting talking points (map decisions to questions)

| Decision made in this project | Question it answers |
|---|---|
| KEDA autoscaling on consumer lag | "How do you scale Kafka consumers under load?" |
| OpenTelemetry + correlation IDs | "How do you trace an event across services?" |
| Prometheus/Grafana + defined SLOs | "How do you know your pipeline is healthy?" |
| ECS Fargate + MSK, EKS manifests validated once | "How do you deploy this without managing servers — and do you know Kubernetes too?" |
| Dead letter topics, retry, circuit breakers | "How do you handle failures?" |
| Avro + Schema Registry + CI compatibility checks | "How do you manage schema evolution safely?" |
| Cost tracked per pipeline stage | "How do you know this is worth what it costs?" (the differentiator most candidates/vendors can't answer) |
| Replication factor / ISR decision + tested recovery | "Walk me through what happens when a broker dies at peak load." |

## Success criteria
- Load test actually run, with real p99/lag/scaling numbers in the repo (not projected)
- A written case study usable, with different framing, for both an interview and a consulting pitch
- Every row in the talking-points table answerable from memory, not notes
