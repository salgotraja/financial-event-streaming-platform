package dev.engnotes.fes.riskalert.rules;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.RiskAlertMetrics;
import dev.engnotes.fes.riskalert.correlation.RecentTrades;
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
 * <p><strong>Each required store is applied to the trade once, before the loop, and only for the
 * stores the rules actually in force declare (ADR-036).</strong> Two separate reasons. Applying
 * inside the loop would apply a store once per governed rule of the same {@code ruleType}, which
 * deduplicating on {@code tradeId} cannot catch because it is one consume call rather than a
 * redelivery. Applying every store unconditionally would make PostgreSQL a hard dependency of every
 * trade, so a deployment governing only {@code price-deviation} would take a database outage it has
 * no reason to take.
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
 * can make the union see no stateful rule in force while the dispatch loop, reading a moment later,
 * sees one. {@code evaluate} therefore resolves every rule type's in-force list into one snapshot up
 * front, and both the union and the dispatch loop read from that same snapshot, so they cannot
 * disagree.
 */
public class RiskRuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskRuleEngine.class);

    private final RiskRuleRegistry registry;
    private final Map<String, RiskRule> rulesByType;
    private final TradeStateStores stores;
    private final RiskAlertMetrics metrics;

    public RiskRuleEngine(RiskRuleRegistry registry, List<RiskRule> rules, TradeStateStores stores,
                          RiskAlertMetrics metrics) {
        this.registry = registry;
        this.rulesByType = rules.stream()
                .collect(Collectors.toMap(RiskRule::ruleType, Function.identity()));
        this.stores = stores;
        this.metrics = metrics;
    }

    public List<RiskAlertEvent> evaluate(EnrichedTradeEvent trade) {
        long instant = trade.getTrade().getEventTimestamp().toEpochMilli();

        // One snapshot of the registry for this call. The union below and the dispatch loop both
        // read from it, never from the registry directly, so a transition applied by the fold
        // thread mid-call cannot make them disagree.
        Map<String, List<ActiveRule>> governedByType = rulesByType.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), ruleType -> registry.inForceAt(ruleType, instant)));

        TradeContext context = applyRequiredStores(trade, requiredKinds(governedByType));

        List<RiskAlertEvent> alerts = new ArrayList<>();
        for (RiskRule rule : rulesByType.values()) {
            for (ActiveRule governed : governedByType.getOrDefault(rule.ruleType(), List.of())) {
                try {
                    evaluateOne(rule, trade, governed, context).ifPresent(alerts::add);
                } catch (InvalidRuleParametersException e) {
                    log.warn("Skipping ruleId={} ruleType={} ruleVersion={} for tradeId={}: {}",
                            governed.ruleId(), governed.ruleType(), governed.version(),
                            trade.getTrade().getTradeId(), e.reason());
                }
            }
        }
        return alerts;
    }

    /**
     * The union of the state kinds declared by the rules actually in force, read from the same
     * snapshot the dispatch loop reads so the two cannot disagree.
     */
    private Set<StateKind> requiredKinds(Map<String, List<ActiveRule>> governedByType) {
        Set<StateKind> required = EnumSet.noneOf(StateKind.class);
        for (RiskRule rule : rulesByType.values()) {
            if (rule instanceof StatefulRiskRule stateful
                    && !governedByType.getOrDefault(rule.ruleType(), List.of()).isEmpty()) {
                required.addAll(stateful.requires());
            }
        }
        return required;
    }

    /**
     * Each required store is applied exactly once, here, before any rule runs. A store whose kind
     * no rule in force declared is never touched, so a deployment governing only price-deviation
     * takes no database dependency it has no reason to take (ADR-036).
     */
    private TradeContext applyRequiredStores(EnrichedTradeEvent trade, Set<StateKind> required) {
        return new TradeContext(
                required.contains(StateKind.POSITION) ? stores.positions().apply(trade) : null,
                required.contains(StateKind.VOLUME_WINDOW) ? stores.volumeWindows().apply(trade) : null,
                required.contains(StateKind.RECENT_TRADES) ? applyRecentTrades(trade) : null);
    }

    private RecentTrades applyRecentTrades(EnrichedTradeEvent trade) {
        RecentTrades recentTrades = stores.recentTrades().apply(trade);
        if (recentTrades.truncated()) {
            metrics.recordCandidateSetTruncated();
        }
        return recentTrades;
    }

    private Optional<RiskAlertEvent> evaluateOne(RiskRule rule,
                                                 EnrichedTradeEvent trade,
                                                 ActiveRule governed,
                                                 TradeContext context) {

        // The union and this dispatch loop read the same snapshot, so a rule found in force here
        // was also seen by the union that decided which stores to apply: every state kind that
        // rule declared in requires() is therefore non-null on every path that reaches here.
        return rule instanceof StatefulRiskRule stateful
                ? stateful.evaluate(trade, governed, context)
                : rule.evaluate(trade, governed);
    }
}
