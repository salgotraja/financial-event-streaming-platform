# The four producers

Four ingestion services, one topic each. They share a shape: a Spring Boot application, a properties
record, a publisher component, and a driver that is off by default. Each driver is gated on the whole
`@Configuration` class rather than on individual beans, so there is one place per service that decides
whether it generates traffic; with the gate off the module is a publisher and nothing else. All durability settings come from `platform-common`,
so each service's `application.yml` configures only its serializers, its registry URL and its topic.

Every one of them writes trace context onto the record headers as well as into the payload, because
NFR-04.1 requires end-to-end tracing across services that may never deserialise the body.

## trade-producer

Covered in detail in [Follow one trade](spine.md). The short version: publishes `TradeEvent` to
`trades.raw` keyed on ticker, copies `traceparent`, `tracestate` and `correlationId` onto headers, and
surfaces a delivery failure to the caller rather than swallowing it.

It also carries a synthetic trade generator, `TradeGenerationDriver`, off unless
`fes.trade-producer.generation.enabled` is `true`. It has the same shape as the market-data rate
driver below: a `SmartLifecycle` on a virtual thread, waking on a fixed interval and emitting the
whole batch due for it. The price is a uniform draw around a fixed reference rather than a model,
deliberately: FR-01.4 specifies a price model for ticks and nothing specifies one for trade prices, so
inventing one would be a claim about market behaviour this platform does not make.

## market-data-simulator

Publishes `MarketDataTickEvent` to `market-data.ticks`, keyed on ticker. Two parts are worth reading:
the price model and the rate driver.

### The price model

`TickGenerator` uses Geometric Brownian Motion with Pareto-distributed volume (ADR-006).

The price step is the exact GBM solution, not the Euler approximation:

```java
double drift = (model.driftAnnual() - (model.volatilityAnnual() * model.volatilityAnnual()) / 2) * dt;
double shock = model.volatilityAnnual() * sqrtDt * random.nextGaussian();
double next = prices[index] * Math.exp(drift + shock);
```

The arithmetic form `S · (1 + mu·dt + sigma·sqrt(dt)·Z)` can produce a negative price on a large
downward draw, which is not a price, and it omits the Ito correction so the series drifts away from
the configured mu.

Volume is drawn by inverse CDF, `x_m / U^(1/alpha)`, with alpha fixed at 1.5 and therefore infinite
variance. The upper tail is deliberately uncapped: heavy-tailed volume is the whole reason ADR-006
chose Pareto over a Gaussian, since a thin-tailed draw would never exercise the FR-04.2 unusual-volume
rule. A zero draw from `nextDouble()` substitutes the smallest positive double rather than truncating
the tail.

Time is annualised over 252 trading days of 6.5 hours, which is the unit the configured drift and
volatility are quoted in.

None of this is a claim about market microstructure fidelity. No number the simulator produces may be
presented as a market observation.

### The rate driver

`TickGenerationDriver` is a `SmartLifecycle` that paces the generator, and it is **off by default**:

```yaml
generation:
  enabled: ${MARKET_DATA_GENERATION_ENABLED:false}
  rate-per-second: ${MARKET_DATA_RATE_PER_SECOND:1000}
  batch-interval: 10ms
```

FR-01.4 describes a load-simulation mode, not boot behaviour, so a service that starts flooding a
broker on startup would be implementing something else.

The driver emits a batch per wake rather than one event per wake. The JVM cannot reliably park for
less than a millisecond, so an event-per-wake loop tops out near 1,000 events/sec, a twentieth of the
FR-01.4 ceiling. It does not await sends: blocking on each future would serialise the loop on broker
round trips. The producer's 64MB buffer is the backpressure boundary, and once it fills, `send` blocks
until `max.block.ms`, which is the correct signal that the target rate exceeds what the broker will
take. A missed deadline is dropped rather than made up, because bursting to catch up pushes a spike
into a broker that has already shown it cannot keep pace.

!!! danger "Configuring a rate is not measuring one"

    Nothing in this service evidences the FR-01.4 ceiling or NFR-01.1. No sustained-throughput run has
    been done. The measured result is Phase 8 work, a load test under the production durability
    profile, and the guide will not report a number before that run exists.

## corporate-action-producer

Publishes `CorporateActionEvent` to `corporate-actions`, keyed on ticker.

Ticker rather than `corporateActionId`, because corporate actions for one instrument are a sequence
rather than independent facts: a split is superseded by its correction, and a dividend by a revised
amount. A downstream consumer reading this stream as scheduled-event context must see the latest state
of a ticker rather than whichever revision landed on the partition it read first.

It carries a startup announcement, `CorporateActionSeeder`, off unless
`fes.corporate-action-producer.seed.enabled` is `true`. Corporate actions are events in the world
rather than a rate, so this is an `ApplicationRunner` publishing once, the shape
`InstrumentMasterSeeder` uses, not a paced driver. A seeded action the validator rejects is logged and
skipped rather than thrown: failing startup over seed data would take the service down when the right
answer is to announce what is well formed and say which was not.

This is the one producer that validates before publishing, and the reason is the schema's shape.
`attributes` is an open `map<string,string>`, so Avro happily accepts a `STOCK_SPLIT` carrying no
split ratio. Downstream, that becomes context a consumer cannot reason about, and the failure surfaces
as a bad investigation rather than a rejected publish.

`CorporateActionValidator` checks two things:

```java
static Set<String> requiredAttributes(CorporateActionType actionType) {
    return switch (actionType) {
        case DIVIDEND_DECLARATION -> Set.of("dividendPerShare");
        case STOCK_SPLIT -> Set.of("splitRatio");
        case RIGHTS_ISSUE -> Set.of("ratio", "subscriptionPrice");
        case EARNINGS_ANNOUNCEMENT -> Set.of();
    };
}
```

and that `effectiveAt` is not before `announcedAt`, since an action cannot take effect before it was
announced.

Validation failure throws rather than returning a failed future. It is a caller bug, detectable before
any broker interaction, and not a delivery outcome.

The required keys for `DIVIDEND_DECLARATION` and `STOCK_SPLIT` come from the schema's own `doc` field.
No document specifies the full set, so `RIGHTS_ISSUE` follows the same shape and
`EARNINGS_ANNOUNCEMENT` requires nothing beyond its dates. That reasoning is written into the class
comment rather than left for a reader to reconstruct.

## reference-data-service

Publishes `InstrumentReferenceEvent` to the compacted `reference-data.instruments`, keyed on
`instrumentId`. It has four rules, and each one exists because compaction changes what a mistake
costs.

**The key is the instrument id.** On a compacted topic the key is the identity of the row, not a
routing hint. See [Topics and schemas](topics.md#two-keying-decisions).

**A null value is never published.** On a compacted topic a null is a tombstone and deletes the
instrument from every subsequent rebuild. This service has no delete path, so a null is always a bug:

```java
Objects.requireNonNull(instrument,
        "instrument reference must not be null: a null value is a tombstone");
```

**Provenance is stamped, not accepted.** FR-10.5 wants reference-data changes traceable to the
producing workload identity. A `producerIdentity` supplied by the caller is a claim the caller makes
about itself, so the configured identity overwrites whatever arrives.

**`referenceVersion` must advance per instrument.** A version that goes backwards resurrects stale
metadata into a rebuild, and nothing fails, the values are just wrong. The guard is in-process:

```java
highestVersion.compute(instrumentId, (_, highest) -> {
    if (highest != null && version <= highest) {
        throw new IllegalArgumentException(...);
    }
    return version;
});
```

It catches a caller republishing an old version within a run. It cannot catch a regression across a
restart, because this service is write-only on the topic and has no consumer to read the current state
back. That limit is a consequence of the permission boundary, not an oversight, and the class comment
says so.

One more difference from the other three producers: `InstrumentReferenceEvent` carries no
`traceContext` field and no `correlationId`. Trace context is therefore injected from the active span
through the Micrometer `Propagator`, and no correlation header is written at all, because an invented
one would correlate nothing.

## The service that is not here

`migration-normalizer` is specified and absent. It would consume the real Debezium PostgreSQL
connector's output from `legacy.trades.cdc` and normalise it into the canonical `TradeEvent`, filling
the four CDC provenance fields that a native trade leaves null. The path uses the real connector; CDC
is not simulated in application code (ADR-020).
