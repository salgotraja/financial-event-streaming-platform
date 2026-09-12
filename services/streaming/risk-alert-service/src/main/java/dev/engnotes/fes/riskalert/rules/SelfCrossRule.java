package dev.engnotes.fes.riskalert.rules;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.riskalert.correlation.RecentTrade;
import dev.engnotes.fes.riskalert.governance.ActiveRule;

/**
 * The self-cross rule: a trader offsetting one of their own prior trades in the same ticker, on the
 * opposite side, within a governed window and within governed quantity and price tolerances.
 *
 * <p><strong>This is not related-party wash detection.</strong> It detects one identity crossing
 * itself: the same {@code traderId} buying and then selling (or the reverse) a matching quantity at
 * a matching price within a short window. The broader definition of a wash trade, distinct but
 * related accounts trading with each other in a way that leaves beneficial ownership unchanged,
 * needs an account-relationship source that {@code contracts/} does not carry and that ADR-030 does
 * not put in scope. The Avro enum symbol stays {@code WASH_TRADE_DETECTED} because it is a published
 * contract, but the narrowing to a single-identity self-cross is recorded here, not only in a design
 * document (ADR-037).
 *
 * <p><strong>The scope is {@code traderId}, not {@code accountId}.</strong> {@link RiskAlertEvent}
 * carries a {@code traderId} field and has no {@code accountId} field, so an account-scoped alert
 * could not name its own subject.
 *
 * <p>Severity is always {@link Severity#CRITICAL}, with no bands: a detected self-cross is not a
 * gradient, the round trip either falls inside the governed tolerances or it does not.
 */
public class SelfCrossRule implements StatefulRiskRule {

    public static final String RULE_TYPE = "self-cross";

    private final long maxWindowSeconds;

    public SelfCrossRule(long maxWindowSeconds) {
        this.maxWindowSeconds = maxWindowSeconds;
    }

    @Override
    public String ruleType() {
        return RULE_TYPE;
    }

    @Override
    public Set<StateKind> requires() {
        return Set.of(StateKind.RECENT_TRADES);
    }

    @Override
    public Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule, TradeContext context) {
        SelfCrossParameters parameters = SelfCrossParameters.from(rule.parameters(), maxWindowSeconds);
        TradeEvent source = trade.getTrade();
        Side triggeringSide = source.getSide();
        long triggeringQuantity = source.getQuantity();
        double triggeringPrice = source.getPrice();

        List<RecentTrade> candidates = context.recentTrades().priorTrades();
        RecentTrade match = null;
        for (RecentTrade candidate : candidates) {
            if (candidate.side() == triggeringSide) {
                continue;
            }

            long secondsBetween = Math.abs(Duration.between(
                    candidate.eventTimestamp(), source.getEventTimestamp()).getSeconds());
            if (secondsBetween > parameters.windowSeconds()) {
                continue;
            }

            if (!withinTolerance(triggeringQuantity, candidate.quantity(), parameters.quantityTolerancePercent())) {
                continue;
            }

            if (!withinTolerance(triggeringPrice, candidate.price(), parameters.priceTolerancePercent())) {
                continue;
            }

            match = candidate;
            break;
        }

        if (match == null) {
            return Optional.empty();
        }

        long secondsBetween = Math.abs(Duration.between(
                match.eventTimestamp(), source.getEventTimestamp()).getSeconds());

        return Optional.of(RiskAlertEvent.newBuilder()
                .setAlertId(IdempotencyKeys.deterministic(
                        source.getTradeId(), rule.ruleId(),
                        Long.toString(rule.version())).toString())
                .setCorrelationId(source.getCorrelationId())
                .setTriggeringTradeId(source.getTradeId())
                .setAlertType(AlertType.WASH_TRADE_DETECTED)
                .setSeverity(Severity.CRITICAL)
                .setTicker(source.getTicker())
                .setTraderId(source.getTraderId())
                .setDescription("Trade " + source.getTradeId() + " in " + source.getTicker()
                        + " offsets a prior trade by the same trader within the governed self-cross window")
                .setRuleParameters(Map.of(
                        SelfCrossParameters.WINDOW_KEY, Long.toString(parameters.windowSeconds()),
                        SelfCrossParameters.QUANTITY_TOLERANCE_KEY,
                        Double.toString(parameters.quantityTolerancePercent()),
                        SelfCrossParameters.PRICE_TOLERANCE_KEY,
                        Double.toString(parameters.priceTolerancePercent())))
                .setMeasuredValues(Map.of(
                        "matched-trade-id", match.tradeId(),
                        "matched-trade-quantity", Long.toString(match.quantity()),
                        "matched-trade-price", Double.toString(match.price()),
                        "matched-trade-side", match.side().toString(),
                        "seconds-between-trades", Long.toString(secondsBetween),
                        "triggering-trade-quantity", Long.toString(triggeringQuantity),
                        "triggering-trade-side", triggeringSide.toString()))
                .setRuleId(rule.ruleId())
                .setRuleVersion(rule.version())
                // Event time, like every other timestamp decision in this service. A wall-clock
                // value would make a replayed alert differ from the original.
                .setAlertTimestamp(source.getEventTimestamp())
                // EnrichedTradeEvent carries no traceContext of its own, so it comes from the
                // wrapped trade. The field defaults to an empty map, so omitting this setter
                // compiles and silently breaks trace propagation across the alert hop.
                .setTraceContext(source.getTraceContext())
                .build());
    }

    /**
     * Relative-tolerance comparison. Candidate values are read back from a {@code NUMERIC(19,4)}
     * column and so are rounded to four decimal places, while the triggering trade's own values
     * come straight off the Avro record; the two sides of this comparison are never exactly equal
     * even for what was originally the same value, so this never compares by equality.
     */
    private static boolean withinTolerance(double triggering, double candidate, double tolerancePercent) {
        if (triggering == 0.0) {
            return candidate == 0.0;
        }
        double relativeDifferencePercent = Math.abs(triggering - candidate) / Math.abs(triggering) * 100.0;
        return relativeDifferencePercent <= tolerancePercent;
    }
}
