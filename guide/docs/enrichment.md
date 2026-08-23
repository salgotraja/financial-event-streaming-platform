# The trade enrichment service

`trade-enrichment-service` is the second service in the deterministic streaming plane and the first
to read from two sources at once. It consumes `trades.raw`, adds market state and instrument
reference data, and writes `trades.enriched`.

It is also the first reader of the market cache. Until this service existed, `market-data-cache-projector`
filled two Redis hashes per ticker that nothing consumed. Both are now read on every trade.

## The shape of the event decides the design

`EnrichedTradeEvent` has no optional fields. Every one of `midPriceAtExecution`, `spreadAtExecution`,
`vwap5Min`, `marketCap`, `priceDeviation`, `marketDataAgeMs`, `enrichedAt` and `enrichmentLatencyMs`
must be computable, or the event cannot be produced at all.

That single constraint explains most of what follows. A trade whose inputs are missing does not get a
partial event with zeroes in the gaps: it takes the dead-letter path. A zero `vwap5Min` would read
downstream as a real traded price of zero, and the risk service that consumes this stream has no way
to tell the difference.

## Two inputs, deliberately different shapes

```yaml
fes:
  trade-enrichment-service:
    topic: trades.raw
    reference-topic: reference-data.instruments
    output-topic: trades.enriched
    market-data-max-age: 30s
    instrument-cache-timeout: 60s
```

`trades.raw` is an ordinary group subscription. The consumer group is `trade-enrichment-service`,
name equal to service name and never suffixed, because that name is what the `GROUP` grant in the
service's own `kafka-acls.yml` scopes.

`reference-data.instruments` is not a subscription at all. It is a bare `assign()` over every
partition, folded into an in-memory map, committing no offsets and joining no group.

The reason is arithmetic. The instrument master is compacted across 6 partitions keyed by
`instrumentId`, while trades arrive on 12 partitions keyed by ticker. The two cannot be aligned, so
any instance can be handed a trade for any ticker, and every instance therefore needs every
instrument. A group subscription would give each instance a subset, which is precisely wrong. The
choice also keeps the Kafka policy smaller: assigning partitions needs no `GROUP` grant, so the
service holds exactly one.

The map is keyed by ticker, because every lookup arrives as a ticker off a `TradeEvent`, and a second
map remembers which ticker each instrument id currently occupies. Without that second map a tombstone,
which carries no value and therefore no ticker, could not find what to remove, and a ticker change
would leave the old symbol enriching trades forever.

## What it computes

Every market-derived field comes from one snapshot, read in a single Lua script over both cache keys.
Two separate reads could straddle a projector write and pair a mid-price from one tick with a window
that already included the next.

```text
midPriceAtExecution = (bidPrice + askPrice) / 2
spreadAtExecution   = askPrice - bidPrice
priceDeviation      = (trade.price - mid) / mid * 100
marketCap           = sharesOutstanding * lastTradedPrice / 1e7      INR crores
marketDataAgeMs     = trade.eventTimestamp - cachedTick.eventTimestamp
vwap5Min            = sum(pv) / sum(v) over buckets in [tradeBucket - 300, tradeBucket]
```

`marketCap` uses the cached tick's `lastTradedPrice` rather than the mid-price or the trade's own
price. Using the trade's own price would let a single odd fill move the reported capitalisation of the
whole instrument, which contradicts what the field means.

The `/1e7` is the crores conversion, and `TradeEnricher` is the only place in the codebase where it
appears.

## Two decisions that keep the output deterministic

Replaying `trades.raw` must produce the same `trades.enriched` records it produced live. Two things in
this service exist for that reason and are easy to get wrong in the same way.

**The VWAP fold is bounded at both ends.** Buckets outside `[tradeBucket - 300, tradeBucket]` are
discarded. The lower bound is the five-minute horizon and is obvious. The upper bound is the one that
matters: without it, replaying an old trade against a warm window folds in ticks that arrived *after*
the trade executed, and produces a different `vwap5Min` than the live run did. This is the reading
half of the same decision that made the projector bucket by event time rather than by a wall clock
(ADR-033).

**The freshness policy is bounded at both ends too.** `marketDataAgeMs` is an event-time difference on
both sides. The entry is usable when `0 <= age <= 30s`. A policy shaped only as "reject anything older
than the limit" passes every negative age trivially, and a negative age means the cached tick postdates
the trade, which is exactly what replaying old trades against a warm cache produces. Such a trade would
be enriched with market data from after it executed (ADR-034).

The injected clock is read for `enrichedAt` and `enrichmentLatencyMs` and nothing else. Those two are
processing telemetry and are expected to differ between a live run and a replay. If the clock reached
the freshness comparison, the same record replayed could flip between enriched and dead-lettered.

`enrichmentLatencyMs` is stamped before the send, so it measures consume to pre-publish and never
consume to broker acknowledgement. It is a processing-cost measure, not a delivery measure.

## When a trade cannot be enriched

Four failure classes, three of them inherited unchanged from the projector's error handler.

| Condition | What happens |
| --- | --- |
| Undecodable payload | not retryable, straight to `trades.raw.dlq` with the bytes the broker delivered |
| Redis connection failure or command timeout | container pauses, unlimited attempts, never dead-letters |
| Trade validation failure | not retryable, `trades.raw.dlq` |
| Reference data unavailable | not retryable, `trades.raw.dlq` |

The last class carries a reason, which is both the metric tag and the dead-letter message:

| reason | Meaning |
| --- | --- |
| `tick_absent` | no tick has ever been projected for this ticker |
| `stale` | the cached tick predates the trade by more than the configured maximum |
| `future` | the cached tick postdates the trade |
| `window_empty` | the rolling window carries no volume in this trade's horizon |
| `instrument_missing` | the instrument master does not carry this ticker |

None of the three non-outage classes is retried. Retrying a decode failure does not improve the bytes,
and every reference-data reason is deterministic for a given record and cache state: `stale` and
`future` are event-time comparisons that cannot change inside a five-second back-off. Leaving them
retryable would triple the Redis reads and the latency for every bad trade.

**A stalled market-data feed therefore dead-letters the trade flow** for as long as the stall lasts,
because every trade breaches the freshness bound. That is a deliberate choice, not an oversight. ADR-027
scopes circuit breakers to calls against a failing dependency, and a Redis read that succeeds and
returns an old value is not a failing call. The mitigation is that
`enrichment_reference_unavailable_total{reason="stale"}` makes it visible, not a mechanism that
suppresses it.

## The readiness gate

If the trade listener started before the instrument master had been read, every trade for an instrument
the process had not reached yet would dead-letter with `instrument_missing`, intermittently enough to
look like a different bug.

What prevents that is not the `autoStartup = "false"` on the listener, and the distinction is worth
stating because the obvious reading is wrong. The instrument load runs inside a blocking
`SmartInitializingSingleton`, and every `SmartInitializingSingleton` in a Spring context completes
during `finishBeanFactoryInitialization()`, strictly before `finishRefresh()`, which is the later phase
in which any listener container's auto-start runs. By the time any container could start, the master has
either finished loading or the loader has thrown and aborted startup. `autoStartup = "false"` is a
second belt: it is what lets the configuration start this specific container explicitly rather than
racing the framework, but it is not the thing holding it back.

The load itself waits on a positional condition: `position(partition) >= endOffset(partition)` for all 6
partitions, against end offsets captured once at startup. The two obvious alternatives are both wrong on
a compacted topic. Waiting for a record count cannot work because nobody knows the count in advance, and
waiting to observe a record at `endOffset - 1` cannot work because compaction can remove the record at
any offset, including that one, so the wait would never end. A partition whose captured end equals its
beginning is already satisfied, which is what lets a legitimately empty master start rather than hang.

On timeout the application fails to start. It does not begin consuming with a partial map, which would
trade one loud failure for a quiet stream of dead letters.

## Identity

```yaml
principal: trade-enrichment-service
allowed:
  - {resourceType: TOPIC, name: trades.raw,                 operations: [READ]}
  - {resourceType: TOPIC, name: reference-data.instruments, operations: [READ]}
  - {resourceType: TOPIC, name: trades.enriched,            operations: [WRITE]}
  - {resourceType: TOPIC, name: trades.raw.dlq,             operations: [WRITE]}
  - {resourceType: GROUP, name: trade-enrichment-service,   operations: [READ]}
```

There is deliberately no grant on `market-data.ticks`. Market state reaches this service through Redis,
and an identity that could also read the tick topic would hold a second route to the same data and a way
around the projection ADR-027 exists to enforce. `TradeEnrichmentServiceAuthorizationTest` proves that
denial alongside two others: it may not write the topic it consumes, and it may not join another
workload's consumer group.

In Redis it is a separate identity from the projector, read-only and scoped to the same key space:

```text
user trade-enrichment-service on >... ~market:* resetchannels +eval +evalsha +hgetall
```

Three commands, all reads. The script uses `HGETALL` on both keys, so `+hget` is not needed and is not
granted. The projector writes this key space and this service reads it, and neither identity can do the
other's job.

## Metrics

```text
trades_enriched_total{status}                    enriched | quarantined
enrichment_latency_ms                            consume to pre-publish
market_data_age_ms                               the freshness input
enrichment_reference_unavailable_total{reason}   the five reasons above
enrichment_instrument_cache_size                 instruments folded from the master
```

Two metric names the specification lists under the projector are satisfied here instead. The projector
emits neither `market_cache_stale_reads_total` nor `market_cache_miss_total`, and says so in its own
`MarketCacheMetrics` javadoc: it never reads, so a miss counter there would report zero misses when it
means no readers. They are the reader's quantities, and they are
`enrichment_reference_unavailable_total` tagged `stale` and `tick_absent`.

`enrichment_instrument_cache_size` counts what was folded from the log. That is not a claim that the log
was complete.

## What this does not prove

- **Nothing consumes `trades.enriched` yet.** The risk alert service and the position read model both
  read this stream and neither exists, so the output is verified by tests and not by a downstream
  consumer in anger. This is the same position the projector was in until this service landed.
- **An empty window and an expired one are indistinguishable.** The window carries a 600-second TTL and
  the tick hash carries none, so a ticker that idled past the TTL and is now repopulating looks exactly
  like one that genuinely traded nothing. Both take the `window_empty` path, so a partial window can
  dead-letter a trade that a complete one would have enriched.
- **Window buckets are pruned relative to the latest tick, not the reading trade.** A trade more than
  300 seconds behind the live feed finds its buckets already gone.
- **The instrument cache is per-process and rebuilt on every start.** Two instances can briefly disagree
  during a reference-data update, and the enriched `marketCap` for the same ticker can differ between
  them for the length of that window.
- **The Redis ACL proves the policy, not the binding.** Its assertions run from the test's own client,
  not from the service.
- **No throughput or latency figure has been measured.** The 50,000 events/sec and sub-200ms numbers in
  the design remain targets.
