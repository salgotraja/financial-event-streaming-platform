# Topics and schemas

The local stack provisions 22 topics and 19 schema subjects. Five of those topics have a producer or a
consumer today, and `market-data.ticks` now has two consumers. The rest exist because the inventory is the architecture's, not the current sprint's,
and a topic that appears the day its service ships would have to be created by hand or by
auto-creation, and auto-creation quietly produces single-partition topics.

## The inventory

Source: `deploy/compose/topics.tsv`. Replication is 3 for every topic, matching
`min.insync.replicas=2` and the producers' `acks=all`.

| Topic | Partitions | Retention | Key | Used today |
| --- | ---: | --- | --- | --- |
| `trades.raw` | 12 | 7 days | ticker | written by `trade-producer`, read by `audit-service` |
| `trades.enriched` | 12 | 7 days | ticker | no |
| `market-data.ticks` | 12 | 1 day | ticker | written by `market-data-simulator`, read by `audit-service` and `market-data-cache-projector` |
| `corporate-actions` | 6 | 30 days | ticker | written by `corporate-action-producer`, read by `audit-service` |
| `reference-data.instruments` | 6 | compacted | instrumentId | written by `reference-data-service`, read by `audit-service` |
| `positions.snapshots` | 12 | 7 days | | no |
| `notifications.alerts` | 6 | 3 days | | no |
| `risk-rules.events` | 6 | 365 days | | no |
| `alert-cases.events` | 6 | 365 days | | no |
| `controls.reconciliation` | 6 | 365 days | | no |
| `security.events` | 6 | 365 days | | no |
| `legacy.trades.cdc` | 6 | 3 days | | no |
| `reconciliation.observations` | 6 | 7 days | | no |
| `anomaly.candidates` | 6 | 3 days | | no |
| `agent.decisions` | 6 | 30 days | | no |
| `review.decisions` | 6 | 365 days | | no |
| `remediation.requested` | 6 | 30 days | | no |
| `precedent.graph.sync` | 6 | 7 days | | no |
| `trades.raw.dlq` | 12 | 30 days | source key | written by `audit-service` on quarantine |
| `market-data.ticks.dlq` | 12 | 30 days | source key | written by `audit-service` and `market-data-cache-projector` on quarantine |
| `corporate-actions.dlq` | 6 | 30 days | source key | written by `audit-service` on quarantine |
| `reference-data.instruments.dlq` | 6 | 30 days | source key | written by `audit-service` on quarantine |

Twelve partitions is the scaling ceiling for trade-path consumer groups. The control-plane topics use
six deliberately: ordering and auditability matter more than throughput there.

Dead-letter topics are source-aligned in partition count, so a replay preserves per-key ordering, and
carry 30 days rather than the source topic's retention, because triage happens after the source
record has expired.

## Two keying decisions

**Trade-path topics key on ticker.** A ticker's events stay on one partition, so a stateful consumer
instance owns that ticker's running state without cross-instance coordination.

**`reference-data.instruments` keys on `instrumentId`.** The topic is compacted, so the key is not a
routing hint but the identity of the row: the broker keeps the latest record per key and discards the
rest. Keying on ticker would make the compacted view a per-ticker view, which breaks the moment one
ticker carries two instruments, and it would silently drop history that a rebuild depends on.

The same compaction has a second consequence. A null value on a compacted topic is a tombstone and
deletes the instrument from every subsequent rebuild, so `InstrumentReferencePublisher` rejects a null
outright.

## Subjects

Source: `deploy/compose/subjects.tsv`. Each row maps a topic to a schema file and, where the schema
embeds another record, to the subject that record is registered under.

Nineteen subjects: fifteen domain subjects and the four dead-letter subjects, all four carrying
`DeadLetterEvent`. The subject name is `{topic}-value`.

One row is different from the rest:

```text
trades.enriched	EnrichedTradeEvent.avsc	dev.engnotes.fes.events.TradeEvent=trades.raw-value
```

`EnrichedTradeEvent` embeds `TradeEvent`, and it registers with a **reference** to
`trades.raw-value` rather than inlining the type. Inlining would give the registry two definitions of
one record and let them drift. Referenced subjects are listed first in the file so the referenced
version resolves when the referring subject is registered.

Three topics have no subject on purpose:

- `legacy.trades.cdc` carries the Debezium connector's own envelope rather than a schema this
  repository owns.
- `remediation.requested` and `precedent.graph.sync` have no schema yet.

## The duplication that exists, and is known

The topic inventory and the subject map exist twice: in the architecture specification, and again in
`deploy/compose/topics.tsv` and `subjects.tsv`.

The stack needs an inventory it can read at provisioning time, so the `.tsv` files are the copy that
runs. Nothing checks that the two agree, and that is recorded as a known gap rather than presented as
a design. When a topic changes, change it in both places in the same commit.

## Consumer group naming

The group name is the service name, with no suffix. `audit-service` consumes as `audit-service`. The
name appears in three places that must match: `application.yml`, the `GROUP` grant in the service's
`kafka-acls.yml`, and any KEDA scaling query. Changing one without the others produces an
authorization failure that reads like a connectivity problem.

## The dead-letter naming rule

`{original-topic}.dlq`, lowercase. Not `.DLT`, which is Spring Kafka's default.

The name is derived in code from `record.topic()`, never configured per service, so a service cannot
ship a name that no replay tool looks at. See `DeadLetterPublisher.DLQ_SUFFIX`.
