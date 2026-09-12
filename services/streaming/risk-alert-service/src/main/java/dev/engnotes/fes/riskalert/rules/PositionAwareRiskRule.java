package dev.engnotes.fes.riskalert.rules;

import java.util.Optional;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import dev.engnotes.fes.riskalert.position.NetPosition;

/**
 * A rule that reads the trader's position after the trade rather than the trade alone.
 *
 * <p><strong>The rule reads the position, it never applies the trade to it.</strong>
 * {@code RiskRuleEngine} iterates every governed {@link ActiveRule} whose {@code ruleType} matches,
 * and several {@code ruleId}s may share one {@code ruleType}: that is how per-ticker thresholds
 * arrive without a schema change. A rule that mutated the position inside {@code evaluate} would
 * apply one trade once per governed rule, inside a single consume call, and deduplicating on
 * {@code tradeId} would not catch it because it is not a redelivery. The engine therefore applies
 * the trade once, ahead of the loop, and passes the result here (ADR-036).
 *
 * <p>A separate subtype rather than a widened {@link RiskRule}, so {@code PriceDeviationRule} and
 * every test written against it stay untouched.
 */
public interface PositionAwareRiskRule extends RiskRule {

    Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule, NetPosition post);

    /**
     * Never called: {@code RiskRuleEngine} dispatches on the subtype. Reaching this is a wiring
     * error in the engine, not a condition a running system can produce, so it fails loudly rather
     * than returning empty and silently never alerting.
     */
    @Override
    default Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule) {
        throw new UnsupportedOperationException(
                ruleType() + " needs the post-trade position and must be dispatched through the "
                        + "three-argument evaluate");
    }
}
