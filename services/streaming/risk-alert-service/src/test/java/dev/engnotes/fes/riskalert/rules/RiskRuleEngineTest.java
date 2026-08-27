package dev.engnotes.fes.riskalert.rules;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.RuleState;
import dev.engnotes.fes.riskalert.governance.BootstrapRuleProperties;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import dev.engnotes.fes.riskalert.governance.RuleTransition;
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
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()));

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
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()));

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
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()));

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
        RiskRuleEngine engine = new RiskRuleEngine(registry, List.of(new PriceDeviationRule()));

        // pd-broken is missing critical-deviation-percent, so PriceDeviationParameters.from rejects
        // it. That must not stop pd-ok, evaluated in the same loop, from alerting on the same trade.
        List<RiskAlertEvent> alerts = engine.evaluate(
                EnrichedTrades.withDeviationAt(6.0, Instant.ofEpochMilli(2_000L)));

        assertThat(alerts).hasSize(1)
                .extracting(RiskAlertEvent::getRuleId)
                .containsExactly("pd-ok");
    }
}
