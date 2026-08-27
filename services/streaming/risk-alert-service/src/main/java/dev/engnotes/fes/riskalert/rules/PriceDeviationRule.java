package dev.engnotes.fes.riskalert.rules;

import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.riskalert.governance.ActiveRule;

/**
 * FR-04.2's price-deviation rule, banded so that FR-04.3's severity carries information.
 *
 * <p>Stateless: {@code trade-enrichment-service} has already computed
 * {@code EnrichedTradeEvent.priceDeviation} as the percentage deviation of the execution price from
 * the mid-price, so this rule reads a number rather than a market.
 *
 * <p>The comparison is on the absolute deviation. A trade six percent below the mid-price is as far
 * off market as one six percent above it, and comparing the signed value would leave every downward
 * breach unalerted.
 */
public class PriceDeviationRule implements RiskRule {

    public static final String RULE_TYPE = "price-deviation";

    @Override
    public String ruleType() {
        return RULE_TYPE;
    }

    @Override
    public Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule) {
        double deviation = trade.getPriceDeviation();
        if (!Double.isFinite(deviation)) {
            // NaN fails every comparison, so falling through would silently produce no alert and
            // the bad record would look like a clean trade. IllegalArgumentException is registered
            // not-retryable, so this quarantines on the first attempt.
            throw new IllegalArgumentException(
                    "priceDeviation is not finite for tradeId=" + trade.getTrade().getTradeId()
                            + ", so no band comparison is meaningful");
        }

        PriceDeviationParameters bands = PriceDeviationParameters.from(rule.parameters());
        double magnitude = Math.abs(deviation);

        Severity severity;
        if (magnitude >= bands.criticalPercent()) {
            severity = Severity.CRITICAL;
        } else if (magnitude >= bands.warnPercent()) {
            severity = Severity.WARNING;
        } else {
            return Optional.empty();
        }

        TradeEvent source = trade.getTrade();
        return Optional.of(RiskAlertEvent.newBuilder()
                .setAlertId(IdempotencyKeys.deterministic(
                        source.getTradeId(), rule.ruleId(),
                        Long.toString(rule.version())).toString())
                .setCorrelationId(source.getCorrelationId())
                .setTriggeringTradeId(source.getTradeId())
                .setAlertType(AlertType.PRICE_DEVIATION)
                .setSeverity(severity)
                .setTicker(source.getTicker())
                .setTraderId(source.getTraderId())
                .setDescription("Execution price deviates " + magnitude
                        + " percent from the mid-price at execution")
                .setRuleParameters(Map.of(
                        PriceDeviationParameters.WARN_KEY, Double.toString(bands.warnPercent()),
                        PriceDeviationParameters.CRITICAL_KEY, Double.toString(bands.criticalPercent())))
                .setMeasuredValues(Map.of(
                        "price-deviation-percent", Double.toString(deviation),
                        "mid-price-at-execution", Double.toString(trade.getMidPriceAtExecution()),
                        "execution-price", Double.toString(source.getPrice())))
                .setRuleId(rule.ruleId())
                .setRuleVersion(rule.version())
                // Event time, like every other timestamp decision in this service. A wall-clock
                // value would make a replayed alert differ from the original.
                .setAlertTimestamp(source.getEventTimestamp())
                // EnrichedTradeEvent carries no traceContext of its own, so it comes from the
                // wrapped trade. The field defaults to an empty map, so omitting this setter
                // compiles and silently publishes an empty map, breaking trace propagation across
                // the alert hop with nothing failing.
                .setTraceContext(source.getTraceContext())
                .build());
    }
}
