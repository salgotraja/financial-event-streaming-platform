package dev.engnotes.fes.riskalert.rules;

import java.util.Optional;
import java.util.Set;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.governance.ActiveRule;

/**
 * A rule that reads per-trade state rather than the trade alone.
 *
 * <p><strong>The rule reads state, it never applies the trade to it.</strong> Several governed
 * {@code ruleId}s may share one {@code ruleType}, so a rule that mutated a store inside
 * {@code evaluate} would move it once per governed rule within a single consume call, and
 * deduplicating on {@code tradeId} would not catch it because that is not a redelivery. The engine
 * applies each required store once, ahead of the loop (ADR-036).
 *
 * <p>Replaces {@code PositionAwareRiskRule}. The single {@code NetPosition} argument became a
 * {@link TradeContext} when a second and third store arrived; a sibling interface per store would
 * have left a rule needing two kinds of state with nowhere to live.
 */
public interface StatefulRiskRule extends RiskRule {

    /** The state kinds this rule reads. The engine applies the union of these across rules in force. */
    Set<StateKind> requires();

    Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule, TradeContext context);

    /**
     * Never called: {@code RiskRuleEngine} dispatches on the subtype. Reaching this is a wiring
     * error in the engine, not a condition a running system can produce, so it fails loudly rather
     * than returning empty and silently never alerting.
     */
    @Override
    default Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule) {
        throw new UnsupportedOperationException(
                ruleType() + " needs per-trade state and must be dispatched through the "
                        + "three-argument evaluate");
    }
}
