package dev.engnotes.fes.riskalert.governance;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The ungoverned bootstrap rule set from {@code application.yml}.
 *
 * <p>It exists because {@code risk-rule-governance-service} is Phase 5, so nothing writes
 * {@code risk-rules.events} yet and a stream-only service would emit no alerts at all. Bootstrap
 * rules are version 0 by definition and apply only while no governed rule of the same
 * {@code ruleType} is in force (ADR-035).
 */
@ConfigurationProperties(prefix = "risk")
public record BootstrapRuleProperties(List<BootstrapRule> rules) {

    public BootstrapRuleProperties {
        rules = rules == null ? List.of() : List.copyOf(rules);
    }

    public record BootstrapRule(String ruleId, String ruleType, Map<String, String> parameters) {
    }
}
