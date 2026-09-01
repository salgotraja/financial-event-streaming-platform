package dev.engnotes.fes.riskalert.rules;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.RuleState;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.governance.BootstrapRuleProperties;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import dev.engnotes.fes.riskalert.governance.RuleTransition;
import dev.engnotes.fes.riskalert.position.NetPosition;
import dev.engnotes.fes.riskalert.position.RiskPositionStore;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskRuleEngineTest {

    private static final Map<String, String> BANDS =
            Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0");

    private static final BootstrapRuleProperties BOOTSTRAP = new BootstrapRuleProperties(List.of(
            new BootstrapRuleProperties.BootstrapRule("price-deviation", "price-deviation", BANDS)));

    @Test
    void a_trade_is_evaluated_against_the_rule_in_force_at_its_own_event_time() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pd-tight", "price-deviation", 1, RuleState.ACTIVE,
                Map.of("warn-deviation-percent", "0.5", "critical-deviation-percent", "1.0"), 5_000L));
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()), neverCalled());

        EnrichedTradeEvent beforeGovernance = EnrichedTrades.withDeviationAt(1.0, Instant.ofEpochMilli(1_000L));
        EnrichedTradeEvent afterGovernance = EnrichedTrades.withDeviationAt(1.0, Instant.ofEpochMilli(6_000L));

        // 1.0 percent is under the bootstrap's 2.0 warning band and over the governed rule's 1.0
        // critical band, so the same deviation produces different verdicts at different event times.
        assertThat(engine.evaluate(beforeGovernance)).isEmpty();
        assertThat(engine.evaluate(afterGovernance)).hasSize(1);
    }

    @Test
    void every_in_force_rule_of_the_type_is_evaluated_and_each_can_alert() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pd-a", "price-deviation", 1, RuleState.ACTIVE, BANDS, 1_000L));
        registry.apply(new RuleTransition("pd-b", "price-deviation", 1, RuleState.ACTIVE, BANDS, 1_000L));
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()), neverCalled());

        List<RiskAlertEvent> alerts = engine.evaluate(
                EnrichedTrades.withDeviationAt(6.0, Instant.ofEpochMilli(2_000L)));

        assertThat(alerts).hasSize(2)
                .extracting(alert -> alert.getAlertId().toString())
                .doesNotHaveDuplicates();
    }

    @Test
    void a_governed_rule_type_with_no_implementation_is_skipped_without_failing() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pl-1", "position-limit", 1, RuleState.ACTIVE,
                Map.of("threshold-shares", "100000"), 1_000L));
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()), neverCalled());

        // Increment 1 has no position-limit implementation. A governed rule ahead of its code must
        // not fail every trade.
        assertThat(engine.evaluate(EnrichedTrades.withDeviationAt(0.1, Instant.ofEpochMilli(2_000L))))
                .isEmpty();
    }

    @Test
    void a_malformed_governed_rule_version_is_skipped_and_does_not_abort_the_other_rules() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pd-broken", "price-deviation", 1, RuleState.ACTIVE,
                Map.of("warn-deviation-percent", "2.0"), 1_000L));
        registry.apply(new RuleTransition("pd-ok", "price-deviation", 1, RuleState.ACTIVE, BANDS, 1_000L));
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()), neverCalled());

        // pd-broken is missing critical-deviation-percent, so PriceDeviationParameters.from rejects
        // it. That must not stop pd-ok, evaluated in the same loop, from alerting on the same trade.
        List<RiskAlertEvent> alerts = engine.evaluate(
                EnrichedTrades.withDeviationAt(6.0, Instant.ofEpochMilli(2_000L)));

        assertThat(alerts).hasSize(1)
                .extracting(RiskAlertEvent::getRuleId)
                .containsExactly("pd-ok");
    }

    private static final Map<String, String> LIMITS = Map.of(
            PositionLimitParameters.WARN_KEY, "10000",
            PositionLimitParameters.CRITICAL_KEY, "50000");

    /**
     * A store that fails the test if it is ever consulted. This is how the guard is proved: a
     * PRICE_DEVIATION-only deployment must not touch PostgreSQL at all.
     */
    private static RiskPositionStore neverCalled() {
        return new RiskPositionStore(null) {
            @Override
            public NetPosition apply(EnrichedTradeEvent event) {
                throw new AssertionError("the position store must not be consulted when no "
                        + "position-aware rule is in force");
            }
        };
    }

    private static RiskPositionStore counting(AtomicInteger calls, long net) {
        return new RiskPositionStore(null) {
            @Override
            public NetPosition apply(EnrichedTradeEvent event) {
                calls.incrementAndGet();
                return new NetPosition("trader-1", "RELIANCE", net);
            }
        };
    }

    @Test
    void the_position_store_is_not_consulted_when_only_a_stateless_rule_is_in_force() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        RiskRuleEngine engine = new RiskRuleEngine(registry,
                List.of(new PriceDeviationRule(), new PositionLimitRule()), neverCalled());

        // Only the price-deviation bootstrap is in force; no position-limit rule is governed.
        assertThat(engine.evaluate(EnrichedTrades.withDeviationAt(6.0, Instant.ofEpochMilli(2_000L))))
                .hasSize(1);
    }

    @Test
    void the_trade_is_applied_exactly_once_even_when_two_position_rules_are_in_force() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pl-a", "position-limit", 1, RuleState.ACTIVE, LIMITS, 1_000L));
        registry.apply(new RuleTransition("pl-b", "position-limit", 1, RuleState.ACTIVE, LIMITS, 1_000L));

        AtomicInteger calls = new AtomicInteger();
        RiskRuleEngine engine = new RiskRuleEngine(registry,
                List.of(new PositionLimitRule()), counting(calls, 60_000L));

        List<RiskAlertEvent> alerts = engine.evaluate(EnrichedTrades.withPosition(
                "t-1", "trader-1", "RELIANCE", Side.BUY, 100L, Instant.ofEpochMilli(2_000L)));

        assertThat(calls)
                .as("one trade is one position movement, however many governed rules read it")
                .hasValue(1);
        assertThat(alerts).hasSize(2)
                .extracting(alert -> alert.getAlertId().toString())
                .doesNotHaveDuplicates();
    }

    @Test
    void a_stateless_and_a_position_rule_both_evaluate_against_the_same_trade() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pl-a", "position-limit", 1, RuleState.ACTIVE, LIMITS, 1_000L));

        AtomicInteger calls = new AtomicInteger();
        RiskRuleEngine engine = new RiskRuleEngine(registry,
                List.of(new PriceDeviationRule(), new PositionLimitRule()), counting(calls, 60_000L));

        // withDeviationAt fixes traderId=trader-1, ticker=RELIANCE and a 6.0 percent deviation,
        // which is over the bootstrap's 5.0 critical band, so both rules alert on one trade.
        List<RiskAlertEvent> alerts =
                engine.evaluate(EnrichedTrades.withDeviationAt(6.0, Instant.ofEpochMilli(2_000L)));

        assertThat(calls).hasValue(1);
        assertThat(alerts).hasSize(2)
                .extracting(alert -> alert.getAlertType().toString())
                .containsExactlyInAnyOrder("PRICE_DEVIATION", "POSITION_LIMIT_BREACH");
    }

    @Test
    void a_position_rule_governed_with_invalid_bands_is_skipped_without_failing_the_trade() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pl-broken", "position-limit", 1, RuleState.ACTIVE,
                Map.of(PositionLimitParameters.WARN_KEY, "10000"), 1_000L));

        RiskRuleEngine engine = new RiskRuleEngine(registry,
                List.of(new PriceDeviationRule(), new PositionLimitRule()),
                counting(new AtomicInteger(), 60_000L));

        // The price-deviation rule still alerts: one bad governed version degrades only itself.
        assertThat(engine.evaluate(EnrichedTrades.withDeviationAt(6.0, Instant.ofEpochMilli(2_000L))))
                .hasSize(1);
    }
}
