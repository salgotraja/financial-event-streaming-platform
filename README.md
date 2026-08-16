# Financial Event Streaming Platform

A security-first Kafka platform for financial event processing: trade enrichment, real-time risk
evaluation, immutable audit, legacy-to-streaming migration, and a bounded, human-gated AI
investigation layer.

All market data and financial identifiers are synthetic. The platform performs no money movement, no
trade execution, and no regulatory reporting.

Under active development. Not every service described here is implemented yet; read the source before
assuming a component exists.

## Learning guide

`guide/` holds a code-accurate walkthrough of what is actually built: the trade-to-archive spine, the
event contracts and their compatibility gate, the per-service Kafka authorization model, the build
gates, the local stack, and a map from each behaviour to the test that proves it. It follows the
source rather than the specification, and marks the difference where they diverge.

```bash
pip install -r guide/requirements.txt
mkdocs serve -f guide/mkdocs.yml
```

## What it does

Trade executions, market-data ticks, corporate actions and instrument reference data enter as Avro
events on Kafka. Trades are enriched with market state, evaluated against governed risk rules, and
projected into position and exposure read models. Every event is archived to immutable, tamper-evident
object storage with signed manifests, and a reconciliation control proves the archive covers the stream.

Separately and asynchronously, deterministic screening promotes a small subset of events to an
investigation queue, where a capability-bounded LLM agent gathers evidence through read-only tools and
prepares a structured case for human review. The agent can propose. It cannot act.

## Architecture

Five planes, with different reliability, latency and cost characteristics:

| Plane | Responsibility |
| --- | --- |
| Ingestion | Synthetic producers and Debezium CDC from a legacy PostgreSQL source |
| Streaming | Enrichment, risk evaluation, position and exposure read models |
| Audit | Immutable archive, signed manifests, reconciliation controls |
| Control | Anomaly screening, alert cases, maker-checker rule governance |
| Agent | Bounded LLM investigation, precedent graph, human review |

**The invariant that governs most design choices:** the agent plane is asynchronous and optional. A
model provider outage, throttling, graph unavailability or budget exhaustion must never block the
deterministic path or add an availability dependency to it. The two planes carry separate service
objectives, and the throughput target belongs to deterministic streaming alone, never to the LLM.

The build enforces the structural half of this: no ingestion, streaming or audit module may depend on
an agent module, on Neo4j, or on an LLM provider.

## Stack

Java 25, Spring Boot 4.1, Gradle 9.5. Apache Kafka with Avro and Confluent Schema Registry.
PostgreSQL, Redis, S3 with Object Lock, Neo4j. Debezium for change capture. OpenTelemetry, Prometheus,
Grafana and Loki for observability. AWS CDK for ECS Fargate, Helm for EKS.

## Getting started

Requires JDK 25 and a running Docker daemon. Integration tests start real Kafka and Schema Registry
containers, so the first run pulls images and takes a few minutes.

```bash
./gradlew build                    # compile, test, and run both build gates
./gradlew test                     # tests only
./gradlew checkPlaneIsolation      # plane dependency rule on its own
./gradlew :contracts:test          # schema compatibility gate on its own
```

## Build gates

Two rules fail the build rather than warn, and both run on every pull request.

**Schema compatibility.** Every Avro schema must stay fully compatible with the accepted baseline in
`contracts/src/test/resources/schema-baseline/`. FULL rather than BACKWARD, because backward
compatibility alone permits field removal, and two schema versions must interoperate during rolling
upgrades. Accepting a deliberate break is explicit: `./gradlew updateSchemaBaseline` produces a
reviewable diff.

**Plane isolation.** The dependency rule described above, checked across the whole module graph.

## Repository structure

Each service is independently deployable with its own container image, workload identity, consumer
group, scaling policy and database schema. They share only the two modules below, and neither carries
business logic. Modules are added as the work reaches them rather than scaffolded in advance.

```text
contracts/            Avro event contracts and generated types
platform-common/      Kafka defaults, shared test infrastructure, cross-cutting conventions
services/
  ingestion/          producers and CDC migration normalizer
  streaming/          enrichment, risk evaluation, position and exposure
  audit/              archive writer and reconciliation
  control/            anomaly screening, cases, rule governance
  agent/              investigation, precedent sync, human review
```

## Scope

Out of scope by design: order management, order routing, execution against real exchanges, settlement,
money movement, automatic correction of financial ledgers, regulatory reporting, licensed market-data
feeds, autonomous agent remediation, and storage of real personal data.
