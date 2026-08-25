package dev.engnotes.fes.riskalert.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates one trade against every rule in force at that trade's own event time.
 *
 * <p>The instant is {@code trade.eventTimestamp}, never the wall clock. That is what makes replay
 * reproduce the original verdict and what keeps the derived alertId stable across a governance
 * change (ADR-035).
 *
 * <p>A governed rule whose {@code ruleType} has no implementation in this increment is skipped
 * rather than failed. Increments 2 and 3 add the position-limit and windowed rules, and a rule
 * governed ahead of its code must not dead-letter every trade in the meantime.
 *
 * <p>A single {@link ActiveRule} whose governed parameters are invalid is skipped the same way.
 * Task 7's fold-time validator is the primary containment, but the YAML bootstrap set never passes
 * through it, and rejecting here is also what stops one bad rule version from aborting evaluation
 * of every other rule for the same trade: a control-plane typo must degrade only itself, not the
 * whole trade (ADR-035).
 */
public class RiskRuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskRuleEngine.class);

    private final RiskRuleRegistry registry;
    private final Map<String, RiskRule> rulesByType;

    public RiskRuleEngine(RiskRuleRegistry registry, List<RiskRule> rules) {
        this.registry = registry;
        this.rulesByType = rules.stream()
                .collect(Collectors.toMap(RiskRule::ruleType, Function.identity()));
    }

    public List<RiskAlertEvent> evaluate(EnrichedTradeEvent trade) {
        long instant = trade.getTrade().getEventTimestamp().toEpochMilli();

        List<RiskAlertEvent> alerts = new ArrayList<>();
        for (RiskRule rule : rulesByType.values()) {
            for (ActiveRule governed : registry.inForceAt(rule.ruleType(), instant)) {
                try {
                    rule.evaluate(trade, governed).ifPresent(alerts::add);
                } catch (InvalidRuleParametersException e) {
                    log.warn("Skipping ruleId={} ruleType={} ruleVersion={} for tradeId={}: {}",
                            governed.ruleId(), governed.ruleType(), governed.version(),
                            trade.getTrade().getTradeId(), e.reason());
                }
            }
        }
        return alerts;
    }
}
