package dev.engnotes.fes.riskalert.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import dev.engnotes.fes.riskalert.position.NetPosition;
import dev.engnotes.fes.riskalert.position.RiskPositionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates one trade against every rule in force at that trade's own event time.
 *
 * <p>The instant is {@code trade.eventTimestamp}, never the wall clock. That is what makes replay
 * reproduce the original verdict and what keeps the derived alertId stable across a governance
 * change (ADR-035).
 *
 * <p><strong>The trade is applied to the position store once, before the loop, and only when a
 * position-aware rule is actually in force (ADR-036).</strong> Two separate reasons. Applying inside
 * the loop would move the position once per governed rule of the same {@code ruleType}, which
 * deduplicating on {@code tradeId} cannot catch because it is one consume call rather than a
 * redelivery. Applying unconditionally would make PostgreSQL a hard dependency of every trade, so a
 * deployment governing only {@code price-deviation} would take a database outage it has no reason to
 * take.
 *
 * <p>A governed rule whose {@code ruleType} has no implementation in this increment is skipped
 * rather than failed. Increment 3 adds the windowed and correlation rules, and a rule governed ahead
 * of its code must not dead-letter every trade in the meantime.
 *
 * <p>A single {@link ActiveRule} whose governed parameters are invalid is skipped the same way.
 * The fold-time validator is the primary containment, but the YAML bootstrap set never passes
 * through it, and rejecting here is also what stops one bad rule version from aborting evaluation
 * of every other rule for the same trade: a control-plane typo must degrade only itself, not the
 * whole trade (ADR-035).
 *
 * <p><strong>The registry is read exactly once per rule type per {@code evaluate} call.</strong>
 * The fold thread that applies governance transitions runs concurrently with this one, so two
 * independent reads of {@link RiskRuleRegistry} can disagree: a transition landing between them
 * can make the guard see no position-aware rule in force while the dispatch loop, reading a moment
 * later, sees one. {@code evaluate} therefore resolves every rule type's in-force list into one
 * snapshot up front, and both the guard and the dispatch loop read from that same snapshot, so they
 * cannot disagree.
 */
public class RiskRuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskRuleEngine.class);

    private final RiskRuleRegistry registry;
    private final Map<String, RiskRule> rulesByType;
    private final RiskPositionStore positions;

    public RiskRuleEngine(RiskRuleRegistry registry, List<RiskRule> rules, RiskPositionStore positions) {
        this.registry = registry;
        this.rulesByType = rules.stream()
                .collect(Collectors.toMap(RiskRule::ruleType, Function.identity()));
        this.positions = positions;
    }

    public List<RiskAlertEvent> evaluate(EnrichedTradeEvent trade) {
        long instant = trade.getTrade().getEventTimestamp().toEpochMilli();

        // One snapshot of the registry for this call. The guard and the dispatch loop below both
        // read from it, never from the registry directly, so a transition applied by the fold
        // thread mid-call cannot make them disagree.
        Map<String, List<ActiveRule>> governedByType = rulesByType.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), ruleType -> registry.inForceAt(ruleType, instant)));

        NetPosition post = anyPositionRuleInForce(governedByType) ? positions.apply(trade) : null;

        List<RiskAlertEvent> alerts = new ArrayList<>();
        for (RiskRule rule : rulesByType.values()) {
            for (ActiveRule governed : governedByType.getOrDefault(rule.ruleType(), List.of())) {
                try {
                    evaluateOne(rule, trade, governed, post).ifPresent(alerts::add);
                } catch (InvalidRuleParametersException e) {
                    log.warn("Skipping ruleId={} ruleType={} ruleVersion={} for tradeId={}: {}",
                            governed.ruleId(), governed.ruleType(), governed.version(),
                            trade.getTrade().getTradeId(), e.reason());
                }
            }
        }
        return alerts;
    }

    private Optional<RiskAlertEvent> evaluateOne(RiskRule rule,
                                                 EnrichedTradeEvent trade,
                                                 ActiveRule governed,
                                                 NetPosition post) {

        // The guard and this dispatch loop read the same snapshot, so a rule found in force here
        // was also seen by the guard that decided whether to apply the trade: post is therefore
        // non-null on every path that reaches here with a position-aware rule.
        return rule instanceof PositionAwareRiskRule positionAware
                ? positionAware.evaluate(trade, governed, post)
                : rule.evaluate(trade, governed);
    }

    /**
     * The guard, evaluated against the snapshot rather than the registry directly so it cannot
     * disagree with the dispatch loop that reads the same snapshot.
     */
    private boolean anyPositionRuleInForce(Map<String, List<ActiveRule>> governedByType) {
        for (RiskRule rule : rulesByType.values()) {
            if (rule instanceof PositionAwareRiskRule
                    && !governedByType.getOrDefault(rule.ruleType(), List.of()).isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
