package dev.engnotes.fes.riskalert.governance;

import java.util.List;
import java.util.Map;

import dev.engnotes.fes.events.RuleState;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskRuleRegistryTest {

    private static final Map<String, String> BANDS =
            Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0");

    private static final BootstrapRuleProperties BOOTSTRAP = new BootstrapRuleProperties(List.of(
            new BootstrapRuleProperties.BootstrapRule("price-deviation", "price-deviation", BANDS)));

    @Test
    void the_bootstrap_rule_applies_when_no_governed_version_is_in_force() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);

        List<ActiveRule> rules = registry.inForceAt("price-deviation", 1_000L);

        assertThat(rules).singleElement()
                .satisfies(rule -> {
                    assertThat(rule.ruleId()).isEqualTo("price-deviation");
                    assertThat(rule.version()).isZero();
                });
    }

    @Test
    void a_governed_version_of_the_same_rule_type_suppresses_the_bootstrap() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        // A different ruleId, which is the case a ruleId-keyed registry would get wrong: it would
        // never outrank a bootstrap keyed by another id, and both would alert on the same trade.
        registry.apply(new RuleTransition("pd-nifty-2026", "price-deviation", 7, RuleState.ACTIVE,
                BANDS, 2_000L));

        assertThat(registry.inForceAt("price-deviation", 2_500L)).singleElement()
                .satisfies(rule -> assertThat(rule.ruleId()).isEqualTo("pd-nifty-2026"));
    }

    @Test
    void the_bootstrap_returns_for_instants_before_the_governed_version_took_effect() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pd-nifty-2026", "price-deviation", 7, RuleState.ACTIVE,
                BANDS, 2_000L));

        assertThat(registry.inForceAt("price-deviation", 1_500L)).singleElement()
                .satisfies(rule -> assertThat(rule.version()).isZero());
    }

    @Test
    void two_governed_rules_of_one_type_are_both_in_force() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pd-largecap", "price-deviation", 1, RuleState.ACTIVE,
                BANDS, 1_000L));
        registry.apply(new RuleTransition("pd-smallcap", "price-deviation", 1, RuleState.ACTIVE,
                BANDS, 1_000L));

        assertThat(registry.inForceAt("price-deviation", 1_500L))
                .extracting(ActiveRule::ruleId)
                .containsExactlyInAnyOrder("pd-largecap", "pd-smallcap");
    }

    @Test
    void a_rule_type_with_no_bootstrap_and_no_governed_version_is_empty() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);

        assertThat(registry.inForceAt("position-limit", 1_000L)).isEmpty();
    }

    @Test
    void a_governed_rule_type_this_increment_cannot_evaluate_is_still_folded() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pl-1", "position-limit", 1, RuleState.ACTIVE,
                Map.of("threshold-shares", "100000"), 1_000L));

        // Increments 2 and 3 add the implementations. A rule governed ahead of its code must not
        // vanish from the timeline in the meantime.
        assertThat(registry.inForceAt("position-limit", 1_500L)).singleElement()
                .satisfies(rule -> assertThat(rule.ruleId()).isEqualTo("pl-1"));
        assertThat(registry.timelineCount()).isEqualTo(1);
    }

    @Test
    void a_retired_governed_rule_does_not_hand_the_type_back_to_the_bootstrap() {
        RiskRuleRegistry registry = new RiskRuleRegistry(BOOTSTRAP);
        registry.apply(new RuleTransition("pd-nifty-2026", "price-deviation", 7, RuleState.ACTIVE,
                BANDS, 2_000L));
        registry.apply(new RuleTransition("pd-nifty-2026", "price-deviation", 8, RuleState.RETIRED,
                BANDS, 3_000L));

        // Governance turned the rule off. Falling back to an ungoverned YAML default here would
        // re-enable alerting that an approver deliberately retired, which is worse than silence.
        assertThat(registry.inForceAt("price-deviation", 3_500L)).isEmpty();
    }
}
