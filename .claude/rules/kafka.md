---
description: Kafka topology, producer/consumer patterns and topic inventory
paths: "**/kafka/**,**/consumer/**,**/producer/**,**/event/**,**/*Consumer.java,**/*Producer.java,**/*Event.java,contracts/**"
---

## Non-Negotiable Rules

- Delivery is **at-least-once** with idempotent producers and deterministic idempotency keys.
  Never introduce Kafka transactions and never describe the system as exactly-once (ADR-019).
- All consumers MUST be idempotent. Duplicates will arrive.
- `enable.auto.commit=false`. Commit offsets explicitly after successful processing via Spring Kafka
  `MANUAL_IMMEDIATE`.
- Do not set `isolation.level=read_committed`. There is no transactional producer; setting it implies
  a write path that does not exist.
- Dead letter topic pattern is `{original-topic}.dlq`, lowercase. Not `.DLT`.
- Serialise primary financial event payloads with **Avro** against Schema Registry. JSON is permitted
  only for control-plane HTTP APIs, structured logs and audit manifest metadata (ADR-002).
- Poison records are quarantined **per record** after bounded retry, and the offset advances. Never
  open an event-type-wide circuit breaker. Circuit breakers protect calls to failing dependencies
  such as Redis or PostgreSQL, nothing else (ADR-027).

## Consumer Group Naming

The group name is the service name, with no suffix. These are fixed by the specification and by the
IAM policies that scope them:

```text
trade-enrichment-service
risk-alert-service
audit-service
market-data-cache-projector
```

New services follow the same rule: group name equals service name. Do not invent a suffix, and do not
change an existing group name without updating the matching IAM policy and KEDA query.

## Consumer Pattern

- Retriable failure: throw and let Spring Kafka retry with exponential backoff, 3 attempts starting
  at 100ms, 5s maximum elapsed.
- Permanent failure: publish a `DeadLetterEvent` to `{topic}.dlq` with original topic, partition,
  offset, payload, failure reason, exception class, retry count and both failure timestamps, then
  acknowledge. Do not block the partition.
- Include `correlationId`, `traceId` and `spanId` in every log line for every message processed.
- Extract W3C TraceContext from message headers and create a child span.

## Producer Pattern

- Production profile: `acks=all`, `enable.idempotence=true`, replication factor 3,
  `min.insync.replicas=2`. An `acks=1` run is a labelled synthetic ceiling experiment only.
- Always set a message key. Trade-path topics key on ticker so a trader's ticker state stays on one
  partition and one consumer instance.
- Use `KafkaTemplate` with an explicit error callback.
- Inject W3C TraceContext headers at produce time.

## Topics

| Topic | Purpose | Retention | Partitions |
| --- | --- | ---: | ---: |
| `trades.raw` | Raw trade execution events | 7 days | 12 |
| `trades.enriched` | Enriched trade events | 7 days | 12 |
| `market-data.ticks` | Price tick stream | 1 day | 12 |
| `corporate-actions` | Corporate actions | 30 days | 6 |
| `reference-data.instruments` | Instrument master, compacted, key `instrumentId` | compacted | 6 |
| `positions.snapshots` | Position and exposure snapshots | 7 days | 12 |
| `notifications.alerts` | Risk alerts | 3 days | 6 |
| `risk-rules.events` | Versioned risk-rule lifecycle | 365 days | 6 |
| `alert-cases.events` | Investigation case lifecycle | 365 days | 6 |
| `controls.reconciliation` | Control and reconciliation results | 365 days | 6 |
| `security.events` | Security and privileged-action events | 365 days | 6 |
| `legacy.trades.cdc` | Debezium CDC envelope from mock legacy source | 3 days | source-aligned |
| `reconciliation.observations` | Synthetic reconciliation observations | 7 days | 6 |
| `anomaly.candidates` | Bounded deterministic candidates for the agent | 3 days | 6 |
| `agent.decisions` | Structured agent decisions | 30 days | 6 |
| `review.decisions` | Human reviewer verdicts | 365 days | 6 |
| `remediation.requested` | Synthetic approved remediation intent | 30 days | 6 |
| `precedent.graph.sync` | Idempotent graph projection updates | 7 days | 6 |
| `{source-topic}.dlq` | Failed processing events | 30 days | source-aligned |

Twelve partitions is the scaling ceiling for trade-path consumer groups. Control-plane topics use
fewer deliberately: ordering and auditability matter more than throughput there.

Authoritative source: `docs/architecture-v1.2.md`. Avro schemas: `docs/specification-v1.2.md`,
namespace `dev.engnotes.fes.events`, files under `contracts/src/main/avro/`.

## Plane Isolation

The agent plane consumes `anomaly.candidates` and never `trades.enriched` directly. Nothing in the
ingestion, streaming or audit services may depend on agent-plane code or block on it (ADR-021,
ADR-026, ADR-028).
