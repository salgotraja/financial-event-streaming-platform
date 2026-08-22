# The market cache projector

`market-data-cache-projector` is the first service in the deterministic streaming plane and the first
service in the platform to hold a datastore connection. Every service before it talked only to Kafka.

It does one thing: it reads `market-data.ticks` and keeps the latest market state per ticker in Redis,
so that trade enrichment can read a price without calling the simulator. That constraint is ADR-027,
and it is a throughput decision rather than a stylistic one: a synchronous call per record on the
enrichment path would put a network round trip inside the hot loop.

Nothing reads the cache yet. Trade enrichment does not exist, so this service currently fills a cache
with no readers. That is the order the phases run in, not an oversight.

## What it consumes and what it writes

```yaml
fes:
  market-data-cache-projector:
    topic: market-data.ticks
    consumer-instance: ${HOSTNAME:market-data-cache-projector-local}
```

The consumer group is `market-data-cache-projector`. Group name equals service name, no suffix, for
the same reason it does on the audit consumer: the name is what the `GROUP` grant in the service's
Kafka policy scopes.

One hash per ticker, at `market:tick:{ticker}`, holding `eventTimestamp`, `bidPrice`, `askPrice`,
`lastTradedPrice`, `volume`, `producedAt` and `correlationId`.

The source-event timestamp is stored beside the value on purpose. A reader applies its own freshness
policy, and this service never decides what "too old" means for anyone else.

## The write only ever moves forward

`src/main/resources/redis/project-tick.lua` is the whole write path:

```lua
local stored = redis.call('HGET', KEYS[1], 'eventTimestamp')
local incoming = tonumber(ARGV[1])

if stored then
    local current = tonumber(stored)
    if incoming < current then
        return -1
    end
    if incoming == current then
        return 0
    end
end
```

Strictly newer applies, equal is treated as already applied, older is discarded. `MarketStateProjection`
maps the three return codes to `ProjectionOutcome.APPLIED`, `DUPLICATE` and `OLDER`.

This is the part worth understanding, because a plain `HSET` looks equivalent and is not.
`MarketDataTickPublisher` keys ticks on the ticker precisely so Kafka orders them within a partition,
and that ordering holds for the first delivery of new records. It does not hold for the things
at-least-once delivery makes routine: a rebalance redelivering uncommitted records, an offset rewind,
or a deliberate rebuild from the topic's retained history. In each of those a last-write-wins
projection installs a stale price and reports success. Nothing fails, the values are just wrong.

Two details follow from that:

**The comparison happens inside the script, not in Java.** A client-side read-then-write races a
second consumer of the same partition during a rebalance, which is the exact window this closes.

**Entries never expire.** A TTL would turn a stalled price feed into a cache miss, and a miss reads as
a cold cache rather than as the fault it is. Staleness stays visible as age instead.

`should_skip_a_tick_older_than_the_stored_one` and `should_skip_a_tick_whose_timestamp_equals_the_stored_one`
in `MarketStateProjectionIntegrationTest` are the tests that hold this. Both assert the stored price
survives, so a reversed or deleted comparison fails them.

## Two failures that look similar and must not be treated alike

ADR-027 separates a bad record from a failing dependency, and this is the first module where both can
actually happen. `ProjectorKafkaConfiguration` wires them apart.

**A record that will not decode is one quarantined record.** `ErrorHandlingDeserializer` wraps the
Avro deserialiser, so a malformed payload arrives at the listener as a record the error handler can
act on rather than failing inside the deserialiser, where it would stall the partition. It is
registered as not retryable, because the bytes do not improve on a second attempt, so the recoverer
runs immediately, publishes to `market-data.ticks.dlq`, and the offset advances.

The bytes that reach the dead-letter topic come off the exception, not off the record:

```java
if (cause instanceof DeserializationException deserialization) {
    return deserialization.getData();
}
```

`ErrorHandlingDeserializer` sets the record value to `null`, and `DeadLetterPublisher` substitutes an
empty array for a null payload. Passing `record.value()` through would therefore quarantine an empty
payload and destroy the only copy of the evidence.
`should_quarantine_a_malformed_record_with_its_original_bytes_and_keep_the_partition_moving` asserts
the quarantined bytes equal the published ones, so that substitution fails the test rather than
passing quietly.

**An unreachable Redis is not a bad record and must never produce a dead letter.** The back-off
function returns an unlimited-attempt back-off for a connection failure or a command timeout, so the
recoverer is never reached, the offset is never committed, and the ticks apply when Redis returns.

The handler that carries out that back-off is the part that is easy to get wrong:

```java
ContainerPausingBackOffHandler pausing = new ContainerPausingBackOffHandler(
        new ListenerContainerPauseService(registry, projectorPauseScheduler));
```

Spring Kafka's default handler sleeps the consumer thread. With unlimited attempts that stops
`poll()` being called, crosses `max.poll.interval.ms`, and gets the consumer evicted from its group:
an outage would become a rebalance storm. Pausing the container keeps the consumer polling and in the
group while it declines to deliver records.

The exception match is deliberately narrow, `RedisConnectionFailureException` and
`QueryTimeoutException` only. Widening it to `RuntimeException` or `DataAccessException` would swallow
the poison-record path and stop quarantine working at all.

## The timeout that makes the outage path exist

```yaml
spring:
  data:
    redis:
      timeout: 2s
      connect-timeout: 2s
```

This is a prerequisite, not tuning. With no command timeout configured, a Redis that accepts the
socket and then stops answering raises nothing at all: the client blocks on the frozen connection, so
the classification above never runs and the consumer thread stops polling anyway. The outage handling
would be unreachable code, and the failure it was written to prevent would happen by a different
route.

Two seconds sits far above normal latency to a local Redis and far below the 300s default
`max.poll.interval.ms`. Nothing in this repository has measured what that latency actually is; treat
the two-second figure as a margin chosen against the poll-interval ceiling, not as evidence of a
target.

`should_not_quarantine_a_valid_tick_when_redis_is_unreachable` pauses the Redis container for longer
than that timeout, then asserts no dead letter, that the listener container actually reports paused,
and that the tick lands once Redis answers again. The middle assertion is what stops the test passing
for the wrong reason: an earlier version of this test lacked a command timeout, so a frozen connection
never raised anything the error handler could classify, and the test would have passed whether or not
any of this machinery existed. With the timeout in place and the paused-container assertion in place,
the test now exercises the classification, the pause, and the resume.

## Metrics

| Series | Instrument | What it tells you |
| --- | --- | --- |
| `market_cache_projection_lag_seconds` | Gauge | how far behind the source event the projection is running |
| `market_cache_entry_age_seconds{ticker}` | Gauge per ticker | age of the stored entry, so a stalled feed is visible |
| `market_cache_stale_writes_total{reason}` | Counter | ticks the compare-and-set declined, `reason` being `older` or `duplicate` |

Two instrument choices in `MarketCacheMetrics` are worth copying rather than rediscovering.

The lag is a Gauge and not a Timer, because a Timer publishes `_seconds_count`, `_seconds_sum` and
`_seconds_max` and never the bare series name.

The per-ticker gauges read from a `ConcurrentHashMap<String, AtomicLong>` field. Micrometer holds the
observed object weakly, so a gauge registered over a captured local reports `NaN` once that local is
collected, and a `NaN` age is indistinguishable from a healthy feed on a dashboard.
`should_keep_reporting_an_entry_age_after_the_recording_call_has_returned` calls `System.gc()` to
catch exactly that.

The reason tag matters for the same class of reason. A duplicate is ordinary at-least-once
redelivery and means nothing is wrong; an older tick under live consumption points at the producer.
An untagged counter would report a healthy rebalance and a broken producer as the same number.

The two read-side series the specification lists for the market cache,
`market_cache_stale_reads_total` and `market_cache_miss_total`, are deliberately absent. They describe
a reader, and a miss counter that nothing can increment reads as "zero misses" when it means "no
readers". They belong to trade enrichment when it lands.

## Its identity, on both connections

Kafka, from `src/main/resources/security/kafka-acls.yml`: READ on `market-data.ticks`, WRITE on
`market-data.ticks.dlq`, READ on the `market-data-cache-projector` group. Nothing else, so it cannot
read the trade stream and cannot write the topic it projects. `MarketDataCacheProjectorAuthorizationTest`
proves one allowed and three denied actions from that committed file, and
`MarketDataCacheProjectorServiceIdentityStackTest` proves the running container presents that
principal rather than an administrator's. [Workload authorization](authorization.md) and
[Service identity](identity.md) cover how both work.

Redis, in the `strict-security` profile only, from `deploy/compose/redis/users.acl.template`:

```text
user default off
user market-data-cache-projector on >__FES_REDIS_PROJECTOR_SECRET__ ~market:* resetchannels +eval +evalsha +hget +hset
```

`user default off` is the Redis counterpart of the broker's `allow.everyone.if.no.acl.found=false`.
Without it an unauthenticated client keeps full access and every restriction below it is bypassable
by simply not authenticating.

Every command in that list is one the projector itself issues. None was added for the healthcheck,
which reuses `+eval` rather than taking a grant of its own. `+eval` is there because the
compare-and-set is a script, which is why the script is committed in the module and reviewed with the
code rather than loaded from configuration.

The `market:` prefix is therefore a security boundary and not only a naming convention. The scope is
enforced inside scripts too: as this user, `HGET other:key somefield` fails with
`NOPERM No permissions to access a key` even though `+hget` is granted, and that pairing, a granted
command refused by the key space, is what demonstrates the scope rather than a denial that any
ungranted command would produce.

The dev profile runs Redis unauthenticated on the private compose network, on the same terms as every
other dev-profile listener. See [The local stack](local-stack.md).
