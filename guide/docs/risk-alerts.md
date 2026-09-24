# The risk alert service

`risk-alert-service` is the third service in the deterministic streaming plane and the first whose
behaviour is governed from outside its own code. It consumes `trades.enriched`, decides which version
of which rule was in force when each trade executed, and writes `notifications.alerts`.

It is also the first consumer of `trades.enriched`. Until this service existed,
[trade enrichment](enrichment.md) wrote a stream that nothing read.

All four of FR-04.2's rules are implemented, across three increments. `PRICE_DEVIATION` arrived in
increment 1 and is stateless. `POSITION_LIMIT_BREACH` arrived in increment 2 and is the first rule in
the platform whose verdict depends on state rather than on the record in front of it, which is why it
also brings the first PostgreSQL store and the first Flyway migration anywhere in this repository.
`UNUSUAL_VOLUME` and `WASH_TRADE_DETECTED` arrived in increment 3 with two more stores.

**`WASH_TRADE_DETECTED` is implemented in a narrowed form, and that matters more than the count of
rules.** It detects one trader identity crossing itself: an offsetting buy and sell on one ticker by
the same `traderId`, inside a governed window, at a similar quantity and price. It does **not** detect
related-party wash trading, trades between distinct but related accounts that leave beneficial
ownership unchanged. That needs a source saying which accounts are related, and `contracts/` carries
an `accountId` but nothing that relates two of them. ADR-030 puts that class of identity data out of
scope by decision rather than by backlog, so the source is not coming. The rule was redefined against
what exists rather than left unbuilt, and any later claim that FR-04 is met should say which
definition it is met against.

The service's evaluation latency is unmeasured. The sub-5ms p99 figure for rule evaluation is a
target, and increment 3 adds two database round trips per trade to the default configuration.

## The problem the timeline solves

A risk rule is not a constant. It is approved, amended and retired by a control plane, and each
change produces a new version on `risk-rules.events`. So "did this trade breach the rule" is
under-specified. The answer depends on *which version of the rule* you ask, and the only defensible
choice is the version that was in force when the trade executed.

That is why this service folds a timeline rather than caching a current value.

```java
public Optional<ActiveRule> inForceAt(long instant)
```

`RuleTimeline` holds every transition for one `ruleId` and answers what was in force at an instant.
`RiskRuleRegistry` holds one timeline per `ruleId` and answers the same question for a rule type.

**The instant is always the trade's own `eventTimestamp`, never the wall clock.** Nothing in the
selection path reads a clock. Replaying a trade from a month ago selects the version that was in force
a month ago and reproduces the verdict the live run reached. A wall-clock read would make a replay
evaluate old trades against today's rules, which is not a replay of anything.

`RuleTimelineTest` pins the edges: `a_version_is_not_in_force_before_its_effective_instant`,
`the_highest_version_effective_at_the_instant_wins`,
`two_transitions_sharing_an_instant_are_ordered_by_version`, and
`a_retirement_turns_the_rule_off_from_its_own_instant_onward`.

## Only two states put a rule in force

`RiskRuleLifecycleEvent` carries five states. Two of them decide what is in force, and the other three
are noise as far as evaluation is concerned:

| State | Effect on the fold |
| --- | --- |
| `ACTIVE` | puts that version in force from its `effectiveAt` |
| `RETIRED` | takes it back out, from its own instant onward |
| `DRAFT` | folded, never in force |
| `PENDING_APPROVAL` | folded, never in force |
| `REJECTED` | folded, never in force |

Folding the other three rather than discarding them is deliberate: they are the maker-checker record,
and a rejected proposal that silently vanished would leave the timeline unable to explain itself.
`a_draft_alongside_a_live_version_does_not_take_the_rule_out_of_force` and
`a_rejected_proposal_does_not_retire_the_live_version` prove they stay inert.

`a_later_activation_reinstates_a_retired_rule` covers the case where a retirement is followed by a
fresh approval, which the timeline handles without special-casing because retirement closes an
interval rather than deleting a rule.

## Two inputs, deliberately different shapes

```yaml
fes:
  risk-alert-service:
    topic: trades.enriched
    rule-topic: risk-rules.events
    output-topic: notifications.alerts
    consumer-instance: ${HOSTNAME:risk-alert-service-local}
    rule-timeline-timeout: 60s
```

`trades.enriched` is an ordinary group subscription, group name equal to service name and never
suffixed, because that name is what the `GROUP` grant scopes.

`risk-rules.events` is a bare `assign()` over every partition, folded in memory, committing no offsets
and joining no group. This is the same shape [trade enrichment](enrichment.md#two-inputs-deliberately-different-shapes)
uses for the instrument master, and for the same reason: every instance needs every rule, so a group
subscription handing each instance a subset would be precisely wrong.

Three things differ from that precedent, and each one matters.

**The topic is not compacted.** `risk-rules.events` carries 365 days of retention, so the fold reads
full history rather than a compacted latest-per-key view. That is not incidental: a compacted view
would keep only the newest record per `ruleId` and destroy exactly the version history the timeline is
built from. `the_full_history_of_a_rule_is_folded_not_just_its_latest_record` is the test that would
fail if someone compacted the topic.

**There is no tombstone path.** On a compacted topic a null value deletes a key. Here a null value is
simply a malformed record, counted and skipped.

**There is no dead-letter topic, and no write grant to make one.** See [Identity](#identity).

## A control-plane typo must not become a plane outage

The fold gates the trade listener, so anything that fails the fold would fail startup. That makes the
error handling here load-bearing in a way it would not be on an ordinary listener.

Three classes of bad governance record, and all three are logged, counted and skipped, leaving the
previously in-force version untouched. The third splits into five reasons, because a parameter set can
fail validation in five distinct ways:

| reason | Meaning |
| --- | --- |
| `malformed_record` | the payload did not decode |
| `null_value` | the record carried no value |
| `missing_parameter` | a required band is absent |
| `unparseable_value` | a band is not a number |
| `not_finite` | a band is `NaN` or infinite |
| `not_positive` | a band is zero or negative |
| `bands_out_of_order` | the critical band is not above the warning band |

The last five are the `reason()` slugs on `InvalidRuleParametersException`, and they are metric labels
as well as log fields.

The decode case is the one worth explaining, because the obvious implementation does not work. Kafka
deserializes inside `poll()`, before any record is handed to application code, so a `SerializationException`
from a corrupt payload never reaches the fold's own catch: it propagates out of `poll()`, out of the
blocking initialiser, and aborts context refresh. The service would fail to start because someone
published one bad byte sequence to a control-plane topic.

What prevents that is `ErrorHandlingDeserializer` wrapping the Avro deserializer on this consumer.
A decode failure then arrives as a null value with the exception on a header, which the fold reads to
tell `malformed_record` from a genuine `null_value`. `an_undecodable_record_is_skipped_and_the_gate_still_opens`
publishes five bytes with no Confluent magic byte and asserts both that the fold completes and that
the rejection is reported under the right reason.

A `ruleType` with no implementation here is accepted unvalidated rather than rejected, because a
delivered increment cannot know what a later one's parameters look like:
`a_governed_rule_type_this_increment_cannot_evaluate_is_still_folded`. The validator dispatches on
`ruleType`, so it now checks `price-deviation` and `position-limit` bands at fold time and still waves
`unusual-volume` through untouched.

Note that `not_finite` applies only to the price-deviation bands, which are parsed as `double`.
Position-limit bands are whole shares parsed as `long`, which cannot be `NaN` or infinite, so an
out-of-range value there is reported as `unparseable_value` instead.

## The bootstrap set, and what suppresses it

`risk-rule-governance-service` is Phase 5. Nothing writes `risk-rules.events` in production, so a
stream-only service would fold an empty timeline and emit no alerts at all. The gap is filled by a
bootstrap set in `application.yml`:

```yaml
risk:
  rules:
    - rule-id: price-deviation
      rule-type: price-deviation
      parameters:
        warn-deviation-percent: "2.0"
        critical-deviation-percent: "5.0"
    - rule-id: position-limit
      rule-type: position-limit
      parameters:
        warn-position-quantity: "10000"
        critical-position-quantity: "50000"
```

Bootstrap rules are version 0 by definition. The suppression rule is the part that is easy to get
wrong:

**A bootstrap rule applies only while no governed version of the same `ruleType` is in force at the
queried instant.** Not per `ruleId`, and not globally.

Keying suppression on `ruleId` would mean a newly governed rule with a new id fires *alongside* the
built-in default rather than replacing it, which is the opposite of what governance means. Keying it
globally would mean governing one rule type silently disabled the defaults for every other. And
evaluating it without an instant would mean a trade timestamped before the governed version took
effect lost the default that was genuinely in force for it, which is
`the_bootstrap_returns_for_instants_before_the_governed_version_took_effect`.

`a_governed_version_of_the_same_rule_type_suppresses_the_bootstrap` uses a different `ruleId` from the
bootstrap entry and still suppresses it. `a_retired_governed_rule_does_not_hand_the_type_back_to_the_bootstrap`
covers the other half: once the control plane has spoken about a rule type, silence from it is not an
invitation to resume the defaults.

## `ruleType` dispatches, `ruleId` identifies

The two are not interchangeable and the distinction runs through the whole service.

`ruleType` selects an implementation. `RiskRuleEngine` holds the `RiskRule` implementations keyed by
`ruleType()` and asks the registry for every rule of that type in force at the trade's event time.

`ruleId` is governance identity. Several governed rules of one type can be in force simultaneously,
each with its own id, version and parameters, and each can produce its own alert:
`two_governed_rules_of_one_type_are_both_in_force` and
`every_in_force_rule_of_the_type_is_evaluated_and_each_can_alert`.

One malformed governed version degrades only itself. The engine catches
`InvalidRuleParametersException` around a single rule's evaluation and moves to the next, so a bad
version cannot abort a trade's evaluation against every other rule:
`a_malformed_governed_rule_version_is_skipped_and_does_not_abort_the_other_rules`.

## The price deviation rule

`PriceDeviationRule` is stateless. Enrichment has already computed
`EnrichedTradeEvent.priceDeviation` as the percentage deviation of the execution price from the
mid-price, so the rule reads a number rather than a market.

```text
magnitude = abs(priceDeviation)
magnitude >= criticalPercent  ->  CRITICAL
magnitude >= warnPercent      ->  WARNING
otherwise                     ->  no alert
```

Both edges are inclusive and the critical test comes first, so a deviation clearing both bands is
CRITICAL rather than WARNING.

The comparison is on the absolute value. A trade six percent below the mid-price is as far off market
as one six percent above it, and comparing the signed value would leave every downward breach
unalerted: `a_negative_deviation_of_the_same_magnitude_alerts_identically`.

**Two bands, where FR-04.2 names one.** A single threshold makes FR-04.3's severity field carry no
information, because every alert would be the same severity. `PriceDeviationParameters` rejects the
specification's single-threshold parameter name rather than quietly accepting it and guessing a second
band: `the_specifications_single_threshold_name_is_not_silently_accepted`.

A non-finite `priceDeviation` throws rather than falling through. `NaN` fails every comparison, so a
fall-through would silently produce no alert and a corrupt record would look like a clean trade:
`a_non_finite_deviation_is_rejected_rather_than_evaluated`.

## The position limit rule, and the state it needs

`POSITION_LIMIT_BREACH` alerts when a trader's net position in one ticker exceeds a governed bound.
A net position is not on the event. It is a running total over every trade that trader has executed
in that ticker, so this rule is where the platform acquires its first relational store.

```text
magnitude = abs(netQuantity)
magnitude > criticalQuantity  ->  CRITICAL
magnitude > warnQuantity      ->  WARNING
otherwise                     ->  no alert
```

**Both edges are strict here, where price deviation's are inclusive.** "Exceeds a threshold" reads as
strict, so a position sitting exactly on a band does not breach. The two rules genuinely differ on
this, which is easy to miss when reading them side by side, so
`a_position_exactly_at_the_critical_band_warns_rather_than_criticals` and
`a_position_exactly_at_the_warning_band_does_not_breach` exist to fail if anyone harmonises them.

The comparison is on the absolute net, for the same reason price deviation compares magnitude: a
10,000-share short carries the same exposure as a 10,000-share long, and comparing the signed value
would leave every short unalerted. The signed net is still carried into `measuredValues`, so a short
reads as a short in the alert.

Bands are whole shares rather than percentages, parsed as `long`. A position is a count and
`TradeEvent.quantity` is a `long`, so a fractional bound is rejected rather than truncated:
`a_fractional_band_is_rejected_because_a_position_is_a_share_count`.

### Two tables, and why the second one exists

```sql
risk_position               -- PK (trader_id, ticker), the running total
risk_position_applied_trade -- PK trade_id, the idempotency ledger
```

The first holds the total. The second is what makes the total safe under at-least-once delivery, and
it does two jobs rather than one.

The obvious job is deduplication. Redelivery is normal (ADR-019), and a running sum is the first
thing in this platform that is not naturally idempotent: applying one trade twice moves the position
twice. `ON CONFLICT (trade_id) DO NOTHING` claims each trade once.

The less obvious job is the one that matters more. The ledger row also stores `net_quantity_after`,
the position that trade produced. A redelivery is answered from that pinned value rather than from
the current total. Without it, deduplication would stop the double-count but not a wrong verdict: a
trade redelivered after later trades had moved the position would be judged against a position it
never saw, and could turn a breach into a non-breach or the reverse.
`a_redelivery_returns_the_historical_net_not_the_current_one` is that property.

Event-time reproducibility is therefore weaker here than for a stateless rule, and the guide says so
rather than claiming parity. `PRICE_DEVIATION` replays to the same verdict from the record alone.
`POSITION_LIMIT_BREACH` replays to the same verdict only because the ledger remembers. Against an
empty store it agrees only if the whole partition is replayed from the same starting point.

What makes the running total tractable at all is the topic key. `trade-producer` keys `trades.raw`
on ticker and enrichment preserves that key, so every trade for a given `(traderId, ticker)` lands on
one partition and is applied by one consumer in offset order. `last_event_timestamp` still takes
`GREATEST` rather than assignment, because records arrive in offset order but `eventTimestamp` can go
backwards, and a sum is order-independent where a maximum is not.

Concurrency control is the upsert's row lock. A `version` column is incremented on every update for
the audit intent of ADR-008, but it is never read as a compare-and-swap: ticker-keyed partitioning
already makes each row single-writer within the consumer group, so a retry loop could never fire.

### The trade is applied once, and only when it must be

```java
TradeContext context = applyRequiredStores(trade, requiredKinds(governedByType));
```

Increment 2 wrote that line as a single guarded call to one store. Increment 3 generalised it,
because three stateful rules do not fit one hardcoded store, but the two decisions inside it are
unchanged and both are still load-bearing.

Each stateful rule declares the state it reads, as a `Set<StateKind>`. The engine unions the kinds
declared by the rules actually in force, applies exactly those stores once each, and passes one
`TradeContext` carrying a field per kind.

**Applying outside the per-rule loop.** `RiskRuleEngine` evaluates every governed rule of a matching
type, and several `ruleId`s may share one `ruleType` — that is how per-ticker thresholds arrive
without a schema change. A rule that moved the position inside its own `evaluate` would move it once
per governed rule, inside a single consume call. Deduplicating on `tradeId` would not catch that,
because it is one trade applied repeatedly within one transaction rather than a redelivery. The bug
would stay invisible until a second position rule was governed, then corrupt every position silently.
`the_trade_is_applied_exactly_once_even_when_two_position_rules_are_in_force` asserts a call count.

**Applying only what some rule asked for.** A store is consulted only when a rule in force declared
its kind, so a deployment governing no stateful rule writes nothing and takes no database dependency
at all. `the_position_store_is_not_consulted_when_only_a_stateless_rule_is_in_force` and
`only_the_stores_the_rules_in_force_declare_are_applied` both use stores that throw if called, so
they fail rather than pass if the union is widened.

Be clear about what that buys today, though: the shipped `application.yml` bootstraps
`position-limit`, `unusual-volume` and `self-cross`, so in the default configuration the union is
never empty, and PostgreSQL is a hard dependency from the first trade. The property belongs to the
requirements union, not to the shipped configuration. It matters for a deployment whose bootstrap set
and governed timeline carry only `price-deviation`, and it is what keeps that deployment possible
rather than describing how this one runs.

Both readers take one snapshot. `evaluate` resolves every rule type's in-force list once and the
guard and the dispatch loop both read that snapshot, never the registry directly. Two independent
reads could disagree, because the rule fold runs on another thread: a transition landing between them
could leave the guard seeing no position rule while the dispatch loop found one, and a rule would be
handed a position that was never computed.

Rules read state and never write it. `StatefulRiskRule` is a subtype of `RiskRule` rather than a
widening of it, so `PriceDeviationRule` and every test written against it stay untouched. It replaced
the earlier `PositionAwareRiskRule`, which took a single position: a sibling interface per store would
have left three near-identical guards in the engine and no home for a rule needing two kinds of
state.

**One accepted consequence.** The order is apply, evaluate, publish, acknowledge. If publishing
exhausts its bound and the record is quarantined, every store the union applied already counts a trade
whose alert never fired, and since increment 3 that can be three stores rather than one. That stands:
the stores record trades that occurred, and alert delivery is a separate concern. A compensating write would be a second write on an already failing path that can also fail,
leaving the position wrong in the other direction with no record of why. The dead letter is the audit
trail.

## The unusual volume rule, and the window it folds

`UNUSUAL_VOLUME` alerts when a trade's quantity sits a governed number of standard deviations above
the mean of recent trade quantities for that ticker.

**Which distribution it compares against is the whole design, and it is not the market's volume.**
The requirement reads "trade volume exceeds 3 standard deviations above the rolling 60-minute mean
for that ticker", and this service reads that as the distribution of *trade quantities* arriving on
`trades.enriched`. That settles where the state lives without a judgement call:
[the market cache projector](projector.md) consumes `market-data.ticks` and never sees a trade, so it
was never a candidate to hold this window even though it already keeps a bucketed rolling window of
its own shape (ADR-037).

```text
threshold = mean + multiplier * standardDeviation
quantity > mean + criticalSigma * sd  ->  CRITICAL
quantity > mean + warnSigma * sd      ->  WARNING
otherwise                             ->  no alert
```

Exceedance is strict, matching the position rule:
`a_quantity_exactly_at_the_warning_multiplier_does_not_alert`.

### The window is bucketed, and the trade is not in its own distribution

```sql
risk_volume_bucket        -- PK (ticker, bucket_start), 60-second (n, sum, sumsq) rollups
risk_volume_ticker        -- PK ticker, the monotonic prune cutoff
risk_volume_applied_trade -- PK trade_id, the replay pin
```

Mean and standard deviation fold from `(n, Σx, Σx²)`, which is additive, so 60 buckets of 60 seconds
fold to exactly the mean and deviation of the individual trade quantities underneath them. Bucket
width changes the fold cost and the pruning granularity, never the statistic.

**The triggering trade is excluded from its own distribution.** The store applies the trade to its
bucket, folds, then subtracts the trade's own contribution. Without that, the first trade in a window
would compare against a sample of one, where the deviation is zero and every value reads as
infinitely anomalous. `the_window_excludes_the_trade_being_evaluated` pins it.

The subtraction is conditional on the trade's own bucket having survived the prune, which is subtler
than it sounds. A trade older than the cutoff has its bucket deleted before the fold runs, so it never
entered the fold and is already excluded; subtracting it again would drive the count below the true
prior count, and a window holding exactly one prior trade would report zero and silence the rule.
`a_late_trade_outside_the_horizon_still_sees_the_priors_that_are_inside_it` fails if that condition is
dropped.

The prune cutoff comes from a per-ticker high-water mark that only ever advances, for the same reason
the position store takes `GREATEST` on its event timestamp: records arrive in offset order but
`eventTimestamp` can go backwards, and a cutoff taken from a late trade would resurrect a pruned
bucket and change every later fold.
`the_prune_cutoff_never_moves_backwards_when_a_late_trade_arrives` is that property.

Replay is pinned the way the position is. `risk_volume_applied_trade` stores the distribution the
trade actually evaluated against, so a redelivery answers from it rather than re-folding a window that
has moved on: `a_redelivery_returns_the_window_the_first_delivery_saw`.

### Two numbers that stop the rule making claims it cannot support

**A thin window does not alert.** `min-sample-count` is governed, must be at least 2, and below it the
rule returns no alert at any quantity. A three-sigma claim computed from a handful of samples is
noise. `a_window_below_the_governed_minimum_sample_does_not_alert_at_any_quantity` covers the
behaviour and `a_window_below_the_governed_minimum_sample_increments_the_below_minimum_counter`
covers the metric, because the two could otherwise drift apart unnoticed. The alert carries the sample
count in `measuredValues` so a reader can see what the claim rests on:
`the_alert_carries_the_sample_the_claim_rests_on`.

**The arithmetic is exact.** The two totals are `NUMERIC(38,0)` and the deviation is computed in
`BigDecimal`. An hour of a busy ticker can push the sum of squares past the exact-integer range of a
`double`, where the naive variance form cancels to a small negative value and its square root is
`NaN`. That would not fail: every comparison against `NaN` is false, so the rule would quietly stop
alerting. `large_quantities_do_not_cancel_to_a_negative_variance` is the regression test, and it uses
deliberately large quantities because a small known distribution passes either way.

### The horizon is configuration, not a governed parameter

```yaml
fes:
  risk-alert-service:
    volume-window-seconds: 3600
```

Governing the horizon per rule would break the window. The prune deletes buckets outside the
horizon, so a governed rule carrying a longer one would fold a window already partly deleted and
return a quietly wrong statistic rather than an error. Writer prune and reader fold must agree on one
number. What stays governed is what changes a verdict and nothing else: the two sigma multipliers and
the minimum sample.

## The self-cross rule, and what it deliberately does not detect

`WASH_TRADE_DETECTED` alerts when the same `traderId` crosses itself: an offsetting buy and sell on
one ticker, inside a governed window, with quantity and price within governed tolerances.

**Read the narrowing in the intro again before using this rule for anything.** It detects one identity
trading against itself. Related-party wash trading, where distinct but related accounts leave
beneficial ownership unchanged, is not detected and cannot be with the events this platform carries.
`SelfCrossRule`'s own Javadoc says so too, because that is where a reader of the source will look.

Scope is `traderId` rather than `accountId` for a reason that decides it rather than merely favours
it: `RiskAlertEvent` carries a `traderId` field and has no `accountId` field, so an account-scoped
alert could not name its own subject.

Severity is `CRITICAL` with no bands. A detected self-cross is not a gradient: the round trip either
falls inside the governed tolerances or it does not.

### Candidates are bounded by arrival order, not by event time

```sql
risk_recent_trade  -- PK trade_id, UNIQUE applied_seq, index (trader_id, ticker, applied_seq DESC)
```

`applied_seq` is a `BIGSERIAL`, and the candidate query bounds on it rather than on
`event_timestamp`. This is the part worth understanding, because the obvious choice is wrong.

Bounding by event time diverges on replay. A trade at t=100 arriving *after* a trade at t=200 is
invisible to the second trade's first delivery and visible to its replay, so the same trade would get
two different verdicts. `applied_seq` is fixed at first insert and preserved by
`ON CONFLICT (trade_id) DO NOTHING`, so the candidate set is identical on every delivery:
`an_out_of_order_older_trade_applied_later_is_not_a_candidate_on_replay` and
`a_redelivery_keeps_the_arrival_order_the_first_delivery_was_given`. That is why this rule needs no
pinned-match column where the volume window needs its pinned totals.

The query still carries an event-time window, and it is two-sided rather than one: a candidate can be
later in event time while earlier in arrival order, and it still qualifies.
`a_prior_trade_later_in_event_time_but_earlier_in_arrival_order_is_still_a_candidate` exists because
without it, deleting the upper bound would leave every other test green.

### The store returns candidates; the rule decides

`RiskRecentTradeStore` reads no governed parameter at all. The engine applies every store before the
dispatch loop, and therefore before any rule's parameters are in hand, so a store that filtered by a
governed window would have to pick one governed `ruleId`'s window on behalf of all of them. The store
returns a bounded candidate set and `SelfCrossRule` filters it.

That bound is capped, and the cap is visible rather than silent. The query asks for one row more than
the cap, so truncation is detectable, and hitting it increments a counter:
`hitting_the_candidate_cap_is_reported_rather_than_silently_truncated`. A cap that quietly dropped the
offsetting trade would produce no alert and no signal, the same failure shape as the `NaN` variance.

For the same reason, a governed `window-seconds` wider than the configured
`recent-trade-horizon-seconds` is rejected rather than accepted and quietly truncated:
`a_governed_window_wider_than_the_configured_horizon_is_rejected`, with
`a_window_equal_to_the_configured_horizon_is_accepted` pinning the boundary.

Prices are compared by relative tolerance, never by equality. A candidate's price has been round-
tripped through a `NUMERIC(19,4)` column while the triggering trade's price is the unrounded `double`
off the Avro record, so the two sides are not symmetrically precise.

### Which tables actually prune, and on which horizon

Two of the four prune, and it is worth being exact about which.

`risk_volume_bucket` prunes at the window horizon, because a bucket outside the window contributes to
no fold and deleting it changes no answer. `risk_recent_trade` prunes at the seven-day
`trades.enriched` retention rather than at any window: it exists to answer a redelivery, and a record
older than the topic's retention cannot be redelivered at all, which is what makes that delete safe.
Its delete is scoped to the `(trader_id, ticker)` of the trade being applied, so a key that stops
trading stops pruning, and its rows sit there until it trades again.

`risk_volume_applied_trade` is never pruned, like `risk_position_applied_trade` before it. Retention
is the horizon it *could* use, but the only column resembling a cutoff is `applied_at`, which is
wall-clock, and this service keeps wall-clock values out of anything that has to replay identically.
A prune would need an event-time column the table does not carry.

## The alert identity is derived, not random

```java
IdempotencyKeys.deterministic(source.getTradeId(), rule.ruleId(), Long.toString(rule.version()))
```

`alertId` is a version-5 name-based UUID over `(tradeId, ruleId, ruleVersion)`. Delivery is
at-least-once (ADR-019), so redelivery of the same trade is normal rather than exceptional, and a
random id would turn each redelivery into a duplicate alert that no downstream consumer could
recognise as the same event. The derived id makes the duplicate detectable:
`redelivering_the_same_trade_produces_the_same_alert_id`, and
`a_different_rule_version_produces_a_different_alert_id` proves the version is genuinely part of the
identity rather than decoration.

This is the property that makes event-time rule selection load-bearing rather than merely tidy. If
selection read the wall clock, the `ruleVersion` folded into the id would depend on when the record
was processed, and the id would stop being reproducible.

`IdempotencyKeys` lives in `platform-common` and joins its components with a separator it refuses to
accept inside a component, so `("ab", "c")` and `("a", "bc")` cannot collide.

`alertTimestamp` is the trade's own event time, not the wall clock:
`the_alert_timestamp_is_the_trades_event_time_not_the_wall_clock`.

## Publish, then acknowledge

Every alert for a trade is published before that trade's offset is acknowledged. The order is the
at-least-once contract: acknowledging first would allow an offset commit for a trade whose alert never
reached the broker, which is silent data loss rather than a duplicate.

`the_alert_is_published_before_the_offset_is_acknowledged` and
`two_alerts_from_one_trade_are_both_published_before_the_acknowledgement` pin the ordering. A trade
that breaches nothing is acknowledged without publishing anything:
`a_trade_that_breaches_nothing_is_acknowledged_without_publishing`.

A metrics failure after a successful publish does not block the acknowledgement, because the record
has already been handled and re-delivering it to fix a counter would produce a duplicate alert for no
gain: `a_metrics_failure_after_a_successful_publish_does_not_prevent_the_acknowledgement`.

Alerts are keyed by ticker, matching the input topic's keying, and the trace context is carried from
the trade onto the alert: `the_trace_context_is_carried_from_the_trade_onto_the_alert` and
`the_trace_headers_survive_the_hop_onto_the_alert_topic`.

**Every severity is published immediately.** The architecture describes batching WARNING and INFO
alerts on a five-second window. That is deferred, and the reason is structural rather than
scheduling: batching means holding an alert past the point where its trade would be acknowledged,
which cannot coexist with `MANUAL_IMMEDIATE` offset commits without an outbox to hold the pending
alerts durably. Publishing immediately is the honest shape until that outbox exists.

## The readiness gate

If the trade listener started before the rule history had been folded, early trades would be
evaluated against an empty registry, fall back to the bootstrap set, and produce alerts under version
0 that a complete fold would have produced under a governed version. Those alerts would carry
different `alertId`s, so the damage would outlive the race.

The fold runs inside a blocking `SmartInitializingSingleton`, which completes during
`finishBeanFactoryInitialization()`, strictly before `finishRefresh()` where any listener container's
auto-start runs. A `SmartLifecycle` then starts the trade listener explicitly.

The `SmartLifecycle` is not interchangeable with a callback from the loader.
`KafkaListenerEndpointRegistry` is populated by Spring Kafka's own `SmartInitializingSingleton`, and
those callbacks run in bean-definition registration order, so a registry lookup from inside the
loader's own callback can return null.

Two tests hold the gate in place at different levels.
`should_call_load_initial_snapshot_on_the_loader_during_context_refresh` proves the mechanism fires
during refresh, without a broker. `GovernedRuleVersion.changes_the_verdict` proves the effect, and it
is the only test on the branch that can: it publishes a governed rule with a 0.2 percent critical
band and then a trade deviating 0.5 percent, which is under the bootstrap's own 2.0 percent warning
band. That trade alerts only if the governed version actually reached the running service. Deleting
the gate bean makes exactly that test fail, and the other eleven stay green, because the trade
listener would still start against a registry holding only the bootstrap set.

On timeout, controlled by `rule-timeline-timeout`, startup fails rather than proceeding with a partial
fold: `the_load_fails_startup_when_it_cannot_reach_the_end_offsets_in_time`. An empty rule topic is
not a failure, it is the current production state, and the gate opens on it:
`the_gate_opens_on_an_empty_rule_topic`.

## When a trade cannot be evaluated

| Condition | What happens |
| --- | --- |
| Undecodable payload | not retryable, straight to `trades.enriched.dlq` |
| Non-finite `priceDeviation` | not retryable, `trades.enriched.dlq` |
| Anything else | bounded retry, then `trades.enriched.dlq` |

Two retries with exponential backoff, then quarantine. The quarantined record's offset is
acknowledged so the partition keeps moving:
`the_recovered_records_offset_is_acknowledged_so_the_partition_keeps_moving` and
`a_malformed_record_is_quarantined_and_the_record_behind_it_is_still_evaluated`.

**A PostgreSQL outage pauses the container rather than dead-lettering.** Increment 1 had no
dependency-outage branch at all, because the service called no external datastore. Increment 2 gave
it one, so the error handler now takes the same shape
[trade enrichment](enrichment.md#when-a-trade-cannot-be-enriched) uses for Redis.

A lost connection or a statement timeout returns an unlimited-attempt back-off, so the recoverer is
never reached and no dead letter is written for a trade that was never bad. A poison bound is for
bytes that cannot improve; an outage bound is for a dependency that comes back. Merging them would
quarantine good trades during a database restart.

The handler is given a `ContainerPausingBackOffHandler` rather than the default, and the difference
decides whether this works. The default sleeps the consumer thread, so `poll()` stops being called,
and an unbounded sleep crosses `max.poll.interval.ms` and gets the consumer evicted from its group.
That turns an outage into a rebalance storm. Pausing keeps the consumer polling and in the group
while it declines to deliver records. `should_pause_the_container_during_a_postgres_outage_rather_than_dead_letter_a_good_trade`
stops the database, publishes a good trade, asserts nothing reaches the DLQ, then restarts it and
asserts the trade is processed.

A constraint violation is deliberately not treated as an outage. It is a verdict on the data, not a
failing dependency, and pausing the whole container on one bad record would invert the two classes.

The outage branch only works because the datasource binds timeouts. `application.yml` sets a Hikari
`connection-timeout` of 5000 milliseconds and PGJDBC `connectTimeout: 5` and `socketTimeout: 10`, both
in seconds. A refused connection fails fast on its own, but a database that accepts the socket and
then stops answering raises nothing at all without a socket timeout: the call would block the
listener thread indefinitely, never reach the back-off function, and cross `max.poll.interval.ms`
anyway. The bound is what converts a hang into an exception the handler can classify.

For a decode failure the quarantined payload comes from the exception rather than the null record
value, because by the time application code sees the record the value is already gone:
`the_quarantined_payload_comes_from_the_exception_not_the_null_record_value`.

## Identity

```yaml
principal: risk-alert-service
allowed:
  - {resourceType: TOPIC, name: trades.enriched,      operations: [READ]}
  - {resourceType: TOPIC, name: risk-rules.events,    operations: [READ]}
  - {resourceType: TOPIC, name: notifications.alerts, operations: [WRITE]}
  - {resourceType: TOPIC, name: trades.enriched.dlq,  operations: [WRITE]}
  - {resourceType: GROUP, name: risk-alert-service,   operations: [READ]}
```

Two READ grants and one GROUP grant, because only one of the two inputs joins a group.

**There is no WRITE grant on `risk-rules.events` and deliberately never will be.** That topic is the
control plane's governance record. A streaming workload that could write it could manufacture the
approved rule version it then evaluates against, which defeats maker-checker entirely.

That absence is what makes the fold's skip-and-continue behaviour the only available design rather
than a preference: this identity could not dead-letter a bad governance record even if it wanted to,
because a `risk-rules.events.dlq` write would be the control-plane write the paragraph above rules
out.

`RiskAlertServiceAuthorizationTest` proves three denials, and the first is the one that matters:
`should_deny_writing_the_governed_rule_topic`, `should_deny_writing_the_topic_it_consumes`, and
`should_deny_joining_a_consumer_group_other_than_its_own`.

`RiskAlertServiceIdentityStackTest` proves the binding rather than the policy: it runs the service's
own container image against a broker with ACLs applied, denied and then granted.

### The database identity

Increment 2 added a second identity to govern, because the service now holds a store. The plain local
stack runs PostgreSQL unauthenticated on the compose network, on the same terms as every other
dev-profile listener. The strict-security overlay is where an absent grant becomes a denial, and it
hardens two things.

**TLS is required, not offered.** `pg_hba.conf` carries only `hostssl` lines for TCP plus one `local`
line for the socket, and no `trust` method anywhere, so a client that negotiates no encryption is
rejected at the connection rather than discouraged by configuration. NFR-05.1 forbids plaintext
database traffic in cloud profiles and the overlay is this repository's cloud stand-in. The
certificate is issued by the same development CA as the broker's, so `ca.pem` is the one truststore a
client needs.

**The service owns its schema and nothing else.** `risk_alert_service` is created `NOSUPERUSER
NOCREATEDB NOCREATEROLE`, owning only the `risk_alert` schema, with `PUBLIC` revoked from `public`.
The bootstrap superuser has no network line in `pg_hba.conf` at all, and since increment 2's review
the two roles no longer share a password.

`RiskAlertDatabaseSecurityIntegrationTest` proves this against a live container rather than by
reading the compose file: a `sslmode=disable` connection is rejected, a `sslmode=verify-ca` connection
succeeds, `pg_roles` really reports `rolsuper=false` for the service role, and the bootstrap superuser
cannot connect over the network. The negative cases pin the specific rejection, so a dead container or
a wrong password fails the test rather than passing it for the wrong reason.
`RiskAlertDatabaseIdentityTest` is the cheap companion: a file-content assertion that runs without
Docker and fails if anyone deletes `ssl=on` from the overlay.

## Metrics

```text
risk_alerts_fired_total{alert_type, severity}         alerts published to notifications.alerts
risk_rule_versions_rejected_total{reason}             governed versions rejected during the fold
risk_alert_trades_quarantined_total                   enriched trades sent to the dead-letter topic
risk_rule_timelines                                   rule timelines folded from risk-rules.events
risk_volume_window_below_minimum_sample_total         windowed evaluations declined for a thin window
risk_self_cross_candidates_truncated_total            candidate queries that hit their row cap
```

The last two exist because both conditions are silent otherwise. A window below its governed minimum
produces no alert, and a truncated candidate set can drop the very offsetting trade that would have
matched; without a counter, each looks exactly like a quiet market.

Those are the rendered Prometheus names. The meters are registered with dot-delimited names,
`risk.alerts.fired` and so on, matching every meter in the two sibling streaming services; the
Prometheus naming convention converts dots to underscores and appends `_total` to a counter.
Registering the rendered name directly is the mistake this arrangement avoids, and `RiskAlertMetrics`
carries the full reasoning.

`risk_rule_timelines` is bound only after the initial fold completes, so it never reports a partial
fold. It counts what was folded from the log, which is not a claim the log was complete.

`every_meter_in_this_class_scrapes_through_a_real_prometheus_registry_without_throwing` exercises them
through an actual `PrometheusMeterRegistry` scrape rather than the in-memory registry the other metric
tests use.

## What this does not prove

- **Nothing consumes `notifications.alerts`.** The alert case service is Phase 4, so the output is
  verified by tests and not by a downstream consumer in anger. This is the position
  [trade enrichment](enrichment.md) was in until this service landed.
- **Nothing writes `risk-rules.events` in production.** `risk-rule-governance-service` is Phase 5.
  Every governed-version path here is exercised by tests that publish to the topic directly, and in a
  running system the bootstrap set is what applies today.
- **`WASH_TRADE_DETECTED` detects self-cross only, not related-party wash trading.** All four of
  FR-04.2's rules now exist, but this one is implemented against a narrowed definition, because
  `contracts/` carries `accountId` and no source relating two accounts. Treat a clean run of this rule
  as evidence that no trader crossed themselves, never as evidence that no wash trading occurred.
- **The position store cannot be rebuilt from Kafka history.** FR-11.5's rebuild-and-reconcile
  requirement is written against `position-exposure-service`, and even when that lands it rebuilds
  that service's own model, not this store. If this store were lost, nothing reconstructs it, which is
  why the local compose service mounts a volume where the Redis cache beside it deliberately does not.
- **The position total duplicates one FR-11 also holds.** [The position read model](positions.md)
  keeps its own totals by account, trader and ticker. The two carry overlapping numbers, and nothing
  reconciles them: no service owns that comparison yet (ADR-038).
- **Neither replay ledger is pruned.** `risk_position_applied_trade` and
  `risk_volume_applied_trade` both grow with trade volume. Redelivery beyond `trades.enriched`'s
  seven-day retention cannot happen, so older rows are dead weight rather than a correctness need,
  but no job removes them and neither table carries an event-time column a prune could safely use.
  `risk_recent_trade` does prune at that horizon, but only for keys that keep trading.
- **`gross_buy` and `gross_sell` are written and never read.** They are carried for FR-11.2. The
  windowed rules did not end up using them: `UNUSUAL_VOLUME` folds its own distribution of trade
  quantities rather than reading the position's gross figures.
- **No throughput figure covers the three database round trips a default deployment now makes.**
  Bootstrapping `position-limit`, `unusual-volume` and `self-cross` means every trade touches all
  three stores before any rule runs.
- **No latency figure has been measured.** The sub-5ms evaluation target and the platform's
  sub-200ms and 50,000 events/sec figures remain targets until Phase 8 measures them.
- **The fold is per-process and rebuilt on every start.** Two instances starting at different moments
  can briefly disagree about the rule set, and the timeline is held in memory with no persistence.
- **The rules consumer follows the topic but nothing proves catch-up latency.** A governed version
  published while the service runs reaches the fold, which
  `a_record_published_after_the_captured_end_offset_reaches_a_running_loader` shows, but how quickly is
  not measured.
