package dev.engnotes.fes.riskalert.rules;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.riskalert.RiskAlertMetrics;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import dev.engnotes.fes.riskalert.window.VolumeWindow;

/**
 * The unusual-volume rule: a trade whose quantity exceeds a governed number of standard deviations
 * above the rolling 60-minute mean for its ticker.
 *
 * <p>The window is trade sizes observed on {@code trades.enriched}, not market tick volume: the
 * distribution this rule compares against is how much this platform's own trades have been trading,
 * not the ticker's total market activity. The triggering trade is excluded from the window it is
 * compared against, because including it would let one large trade inflate the very distribution
 * that is supposed to flag it. {@link dev.engnotes.fes.riskalert.window.RiskVolumeWindowStore}
 * performs that exclusion before this rule ever sees the window.
 *
 * <p>The 60-minute horizon is configuration for the window store, not a governed parameter of this
 * rule: changing it changes what "unusual" means against a fixed historical shape, which is a
 * deployment decision, not one a rule approver bands.
 */
public class UnusualVolumeRule implements StatefulRiskRule {

    public static final String RULE_TYPE = "unusual-volume";

    private final RiskAlertMetrics metrics;

    public UnusualVolumeRule(RiskAlertMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public String ruleType() {
        return RULE_TYPE;
    }

    @Override
    public Set<StateKind> requires() {
        return Set.of(StateKind.VOLUME_WINDOW);
    }

    @Override
    public Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule, TradeContext context) {
        UnusualVolumeParameters bands = UnusualVolumeParameters.from(rule.parameters());
        VolumeWindow window = context.volumeWindow();

        if (window.sampleCount() < bands.minSampleCount()) {
            metrics.recordWindowBelowMinimumSample();
            return Optional.empty();
        }

        BigDecimal mean = window.mean();
        BigDecimal standardDeviation = window.standardDeviation();
        BigDecimal warnThreshold = mean.add(
                standardDeviation.multiply(BigDecimal.valueOf(bands.warnSigma())));
        BigDecimal criticalThreshold = mean.add(
                standardDeviation.multiply(BigDecimal.valueOf(bands.criticalSigma())));

        TradeEvent source = trade.getTrade();
        BigDecimal quantity = BigDecimal.valueOf(source.getQuantity());

        Severity severity;
        BigDecimal crossedThreshold;
        if (quantity.compareTo(criticalThreshold) > 0) {
            severity = Severity.CRITICAL;
            crossedThreshold = criticalThreshold;
        } else if (quantity.compareTo(warnThreshold) > 0) {
            severity = Severity.WARNING;
            crossedThreshold = warnThreshold;
        } else {
            return Optional.empty();
        }

        return Optional.of(RiskAlertEvent.newBuilder()
                .setAlertId(IdempotencyKeys.deterministic(
                        source.getTradeId(), rule.ruleId(),
                        Long.toString(rule.version())).toString())
                .setCorrelationId(source.getCorrelationId())
                .setTriggeringTradeId(source.getTradeId())
                .setAlertType(AlertType.UNUSUAL_VOLUME)
                .setSeverity(severity)
                .setTicker(source.getTicker())
                .setTraderId(source.getTraderId())
                .setDescription("Trade quantity of " + source.getQuantity() + " in " + source.getTicker()
                        + " exceeds the governed volume band over the rolling window")
                .setRuleParameters(Map.of(
                        UnusualVolumeParameters.WARN_KEY, Double.toString(bands.warnSigma()),
                        UnusualVolumeParameters.CRITICAL_KEY, Double.toString(bands.criticalSigma()),
                        UnusualVolumeParameters.MIN_SAMPLES_KEY, Long.toString(bands.minSampleCount())))
                .setMeasuredValues(Map.of(
                        "window-sample-count", Long.toString(window.sampleCount()),
                        "window-mean-quantity", mean.stripTrailingZeros().toPlainString(),
                        "window-standard-deviation", standardDeviation.stripTrailingZeros().toPlainString(),
                        "triggering-trade-quantity", Long.toString(source.getQuantity()),
                        "crossed-threshold", crossedThreshold.stripTrailingZeros().toPlainString()))
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
}
