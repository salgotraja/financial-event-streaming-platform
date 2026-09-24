# The position and exposure read model

`position-exposure-service` is the fourth and last service of the deterministic streaming plane, and
the first in the platform whose purpose is to be *queried* rather than to decide anything. Every
service before it either makes a decision or feeds one that does. This one consumes
`trades.enriched`, maintains a running position per account, trader and ticker, and publishes
`positions.snapshots`.

It is also the second reader of `trades.enriched`. [The risk alert service](risk-alerts.md) reads the
same stream, and the two do not coordinate: they are independent consumer groups with independent
stores.

Its arrival completes Phase 2. `checkPlaneIsolation` now inspects twelve modules.

## What it is not

Three things, and each one matters more than the feature list.

**It is not a settlement ledger.** It reports what trades imply about a position. Nothing here
settles, nets for clearing, or carries an authoritative balance.

**Its `marketValue` is not a live mark-to-market figure.** It is the net quantity times the mid price
carried on the trade that last moved the position. A position whose ticker has not traded for an hour
is valued at the price it last traded at, not at anything current. The section below explains why that
was the right choice and what it costs.

**It is not queryable over HTTP.** FR-11.4 asks for a read-only API for authorised risk and compliance
users, and no such API exists. That is a boundary rather than a backlog item, explained under
[What this does not prove](#what-this-does-not-prove).

## The grain, and why the upsert is safe

The read model is keyed on `(accountId, traderId, ticker)`. That is FR-11.1's "by account, trader, and
ticker" read literally, and exactly the key `PositionSnapshotEvent` already carried before this
service existed.

```sql
position               -- PK (account_id, trader_id, ticker), the running totals
position_applied_trade -- PK trade_id, the idempotency ledger
```

The grain is not only a reporting choice. `trades.enriched` is keyed on ticker, and ticker is one of
the three components, so every row of `position` is written by one consumer within the group, in
offset order. That single-writer property is what lets the running total be an `ON CONFLICT DO UPDATE`
upsert whose concurrency control is the row lock, rather than optimistic locking with a retry loop
that could never fire. It is the same reasoning [the risk alert service](risk-alerts.md) uses for its
own position table (ADR-036, ADR-038).

Three tests pin the grain rather than assuming it: `the_grain_is_the_three_part_position_key` on the
schema, and `the_grain_separates_two_accounts_trading_one_ticker` and
`the_grain_separates_one_account_trading_two_tickers` on the store.

## The ledger pins every published figure, not just one

`position_applied_trade` does the two jobs its counterpart in the risk service does. It deduplicates
under at-least-once delivery, and it pins what the trade produced so a redelivery answers from the
pinned row rather than from a total that later trades have moved.

It differs in one way that is worth understanding, because the difference is deliberate. The risk
service's ledger pins only the net quantity, since the net is the only figure its rule reads. This one
pins all four: net, gross buy, gross sell and market value.

The reason is that the whole snapshot is this service's output. If a redelivery recomputed any single
field, it would republish a snapshot differing from the original in a field nobody was watching.
`a_redelivery_reproduces_every_figure_the_first_delivery_produced` is that property, and
`the_ledger_pins_every_figure_the_snapshot_publishes` pins the schema that supports it.

`market_value` is `NUMERIC(19,4)`, not a floating-point type, and
`market_value_is_exact_numeric_rather_than_floating_point` fails if anyone changes that. It is a money
figure that the rebuild path will one day compare between a live and a rebuilt model, and binary
floating-point error must not enter that comparison.

## Mark-to-last-trade, and what it costs

```text
marketValue = netQuantity * midPriceAtExecution   (of the trade being applied)
```

The obvious alternative was reading the Redis market cache for a current price. It was rejected for
two reasons, and the second is the stronger one.

It would add a Redis dependency to a service that otherwise needs none, of exactly the kind ADR-027
kept out of the enrichment path. And it would make a replayed trade produce a different `marketValue`
than the original, breaking the property that a replayed event reproduces its original result, which
the streaming plane spends considerable effort preserving elsewhere.

So the valuation is honest about what it is: what the last trade implied. `market_value` is also the
one column where a late-arriving trade overwrites a newer valuation, because the quantity totals are
order-independent sums and `last_event_timestamp` takes `GREATEST`, but a single assigned price
cannot do either. It is mark-to-last-*applied*-trade, and carrying a second timestamp to fix that
would buy precision in a number that is already an approximation.

`market_value_is_the_net_at_the_mid_price_of_the_trade_that_moved_it` pins the behaviour, and
`an_earlier_event_timestamp_delivered_later_does_not_move_last_event_timestamp_backwards` pins the
`GREATEST` that a plain assignment would silently break.

## One snapshot per trade, keyed on the position

Every applied trade publishes one `PositionSnapshotEvent`, before the offset is acknowledged. If that
order inverted, a crash between acknowledging and publishing would lose a snapshot whose offset was
already committed, and nothing would replay it.
`the_snapshot_is_published_before_the_offset_is_acknowledged` fails if the two are swapped.

The record key is `accountId|traderId|ticker`, the composite position key, not the ticker the input
topic uses. Keying on ticker would put unrelated positions' snapshots on one partition with no
ordering between them, and a consumer folding the topic would be at the mercy of interleaving.
`the_record_is_keyed_on_the_composite_position_key` holds it.

`asOf` is the trade's event time and `snapshotId` is derived from the trade and the position key, so a
replayed trade republishes an identical snapshot rather than a new one:
`the_snapshot_id_is_derived_so_a_replay_republishes_the_same_identity`.

**Snapshot volume equals trade volume.** Every trade produces one. That is the simplest thing that
satisfies the requirement, and with nothing consuming the topic yet there is no evidence on which to
tune a material-change threshold, so none was added.

## Failure handling

Unchanged from the rest of the streaming plane, which is the point rather than an omission. A
malformed record is quarantined per record to `trades.enriched.dlq` after the shared bounded retry,
and the record behind it is still applied:
`a_malformed_record_is_quarantined_and_the_record_behind_it_is_still_applied`. A database outage
pauses the listener container instead of dead-lettering a trade the service simply could not evaluate.
Because the apply is `@Transactional`, a pool timeout arrives as `CannotCreateTransactionException` at
transaction begin, and a connection lost mid-statement as `DataAccessResourceFailureException`, so the
classifier matches those, a statement timeout, and any `SQLException` in SQLState class `08`, anywhere
in the cause chain. `should_pause_the_container_during_a_postgres_outage_rather_than_dead_letter_a_good_trade`
holds PostgreSQL paused for well past the poison budget and watches the DLQ for the whole window,
because every back-off pauses the container and a paused container alone proves nothing about which
class the failure was put in.

A failed snapshot publish is treated the same way. The trade is already applied when the send fails,
so the failure says nothing about the trade: `PositionSnapshotPublisher` rethrows it as a
`SnapshotPublishException` and the container pauses until the send succeeds.
`a_failed_snapshot_publish_pauses_and_retries_rather_than_dead_lettering_an_applied_trade` starts the
service with the output topic's schema subject unregistered, which fails every send because the service
runs with `auto.register.schemas=false`, and asserts nothing is dead-lettered and that the snapshot
arrives once the subject is registered. Building the snapshot stays outside that wrap, so a key the
idempotency guard rejects is still a payload verdict:
`a_snapshot_that_cannot_be_built_stays_a_payload_verdict_rather_than_a_publish_failure`.

The apply is one transaction. `a_position_upsert_failure_after_the_claim_insert_rolls_back_the_whole_apply`
forces a failure between the claim row and the figures being filled in, and asserts no orphaned ledger
row survives, because a claim row without its figures would mark a trade applied whose totals were
never written.

## Identity

Workload identity and consumer group are both `position-exposure-service`. Its committed policy grants
one READ on `trades.enriched`, WRITE on `positions.snapshots` and the dead-letter topic, and READ on
its own group. There is deliberately no READ grant on `positions.snapshots`: this service writes that
topic and never reads it, and the rebuild path will replay `trades.enriched` rather than its own
output.

Three denials are tested rather than assumed: `should_deny_writing_the_topic_it_consumes`,
`should_deny_reading_the_topic_it_writes`, and `should_deny_joining_a_consumer_group_other_than_its_own`.

In the strict-security profile it connects over TLS as a role owning only its own schema, with no
superuser, createdb or createrole privilege. One detail there is easy to get wrong and is worth
copying: the `position_exposure` database is owned by the bootstrap role, not by the service. From
PostgreSQL 15 a database's `public` schema is owned through `pg_database_owner`, so making the service
the database owner would have handed it CREATE on `public` and contradicted the least-privilege claim
the rest of the arrangement makes. `the_role_cannot_create_a_table_in_the_public_schema` is the test
that holds it, rather than the reasoning holding it alone.

## Metrics

```text
position_snapshots_published_total          snapshots published to positions.snapshots
position_exposure_trades_quarantined_total  enriched trades sent to the dead-letter topic
```

Those are the rendered Prometheus names. The meters are registered with dot-delimited names, matching
every sibling streaming service.

## What this does not prove

- **The model cannot rebuild itself.** FR-11.5 asks for a full rebuild from Kafka history and a
  reconciliation result, and neither exists. If this store were lost, nothing here reconstructs it,
  which is why its compose service mounts a volume where the Redis cache beside it deliberately does
  not.
- **There is no query API, and that is a boundary, not a backlog item.** FR-11.4 asks for a read-only
  API for authorised risk and compliance users. That is human authorisation, and ADR-030 puts
  workforce identity outside this platform by decision rather than by omission. Building the endpoint
  without it would mean serving `traderId` and `accountId`, both marked RESTRICTED in the schemas,
  with no control in front of them. The data is reachable today only through a Kafka grant on
  `positions.snapshots`.
- **Realised and unrealised exposure are absent.** FR-11.2 asks for them "where applicable". Both need
  a cost basis, and FIFO versus average cost is an accounting policy that nothing in this platform has
  chosen. Picking one silently would present an unmade accounting decision as a derived fact.
- **Nothing consumes `positions.snapshots`.** The output is verified by tests, not by a downstream
  consumer in anger, which is the position [the risk alert service](risk-alerts.md) is still in too.
- **These totals duplicate the risk service's, and nothing reconciles them.** That service's position
  table is narrower on purpose, and the two will drift without anything detecting it until the rebuild
  path exists.
- **`position_applied_trade` is never pruned.** It joins the two unpruned ledgers the risk service
  already carries. All three grow with trade volume, and none carries an event-time column a prune
  could safely use.
- **No latency or throughput figure has been measured.** The platform's sub-200ms and 50,000 events/sec
  figures remain targets, and this service adds a database round trip and a Kafka publish per trade
  to a plane that has never been measured under load.
