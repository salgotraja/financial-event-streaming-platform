# The risk alert service

`risk-alert-service` is the third service in the deterministic streaming plane and the first whose
behaviour is governed from outside its own code. It consumes `trades.enriched`, decides which version
of which rule was in force when each trade executed, and writes `notifications.alerts`.

It is also the first consumer of `trades.enriched`. Until this service existed,
[trade enrichment](enrichment.md) wrote a stream that nothing read.

Increment 1 implements one rule, `PRICE_DEVIATION`. The other three rules FR-04.2 names are absent,
so FR-04 is not met. What this increment establishes is the machinery the other three will land into:
the governed version timeline, event-time selection, and a deterministic alert identity.

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

Three ways a governance record can be bad, and all three are logged, counted and skipped, leaving the
previously in-force version untouched:

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

A `ruleType` this increment has no implementation for is accepted unvalidated rather than rejected,
because increment 1 cannot know what increment 3's parameters look like:
`a_governed_rule_type_this_increment_cannot_evaluate_is_still_folded`.

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

## The rule itself

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
during refresh, without a broker. The round trip
`a_breaching_trade_produces_an_alert_through_a_real_broker_and_registry` proves the effect: deleting
the gate bean makes exactly that test fail, because the trade is evaluated before the governed
version is in force.

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

There is no dependency-outage branch and no container-pausing back-off handler, unlike
[trade enrichment](enrichment.md#when-a-trade-cannot-be-enriched). That machinery exists there to
protect a Redis outage, and this service calls no external datastore, so there is no failing
dependency for a breaker to protect. ADR-027 scopes breakers to calls against a failing dependency,
and copying the branch in without one would be an event-type-wide breaker by another name.

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

## Metrics

```text
risk_alerts_fired_total{alert_type, severity}    alerts published to notifications.alerts
risk_rule_versions_rejected_total{reason}        governed versions rejected during the fold
risk_alert_trades_quarantined_total              enriched trades sent to the dead-letter topic
risk_rule_timelines                              rule timelines folded from risk-rules.events
```

Those are the rendered Prometheus names. The meters are registered with dot-delimited names,
`risk.alerts.fired` and so on, matching every meter in the two sibling streaming services; the
Prometheus naming convention converts dots to underscores and appends `_total` to a counter.
Registering the rendered name directly is the mistake this arrangement avoids, and `RiskAlertMetrics`
carries the full reasoning.

`risk_rule_timelines` is bound only after the initial fold completes, so it never reports a partial
fold. It counts what was folded from the log, which is not a claim the log was complete.

`every_meter_in_this_class_scrapes_through_a_real_prometheus_registry_without_throwing` exercises all
four through an actual `PrometheusMeterRegistry` scrape rather than the in-memory registry the other
metric tests use.

## What this does not prove

- **Nothing consumes `notifications.alerts`.** The alert case service is Phase 4, so the output is
  verified by tests and not by a downstream consumer in anger. This is the position
  [trade enrichment](enrichment.md) was in until this service landed.
- **Nothing writes `risk-rules.events` in production.** `risk-rule-governance-service` is Phase 5.
  Every governed-version path here is exercised by tests that publish to the topic directly, and in a
  running system the bootstrap set is what applies today.
- **Three of FR-04.2's four rules are absent, so FR-04 is not met.** `POSITION_LIMIT_BREACH`,
  `UNUSUAL_VOLUME` and `WASH_TRADE_DETECTED` are later increments. Wash-trade detection is additionally
  blocked on a definition: `contracts/` carries `accountId` but no account-relationship source, so the
  rule as specified cannot be implemented against the events that exist.
- **No latency figure has been measured.** The sub-5ms evaluation target and the platform's
  sub-200ms and 50,000 events/sec figures remain targets until Phase 8 measures them.
- **The fold is per-process and rebuilt on every start.** Two instances starting at different moments
  can briefly disagree about the rule set, and the timeline is held in memory with no persistence.
- **The rules consumer follows the topic but nothing proves catch-up latency.** A governed version
  published while the service runs reaches the fold, which
  `a_record_published_after_the_captured_end_offset_reaches_a_running_loader` shows, but how quickly is
  not measured.
