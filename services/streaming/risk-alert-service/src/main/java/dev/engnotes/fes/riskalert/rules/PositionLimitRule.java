package dev.engnotes.fes.riskalert.rules;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.riskalert.governance.ActiveRule;

/**
 * FR-04.2's position-limit rule: a single trader's net position in a ticker exceeding a configurable
 * threshold. Banded, so FR-04.3's severity carries information.
 *
 * <p><strong>The comparison is on the absolute net.</strong> FR-04.2's "exceeds a configurable
 * threshold" does not settle the short case, and a 10,000-share short carries the same exposure as a
 * 10,000-share long, so one bound governs both directions. The signed net is carried into
 * {@code measuredValues} alongside the magnitude, so a short still reads as a short (ADR-036).
 *
 * <p>Strict exceedance: a position exactly at the band does not breach, matching the plain reading
 * of "exceeds".
 */
public class PositionLimitRule implements StatefulRiskRule {

    public static final String RULE_TYPE = "position-limit";

    @Override
    public String ruleType() {
        return RULE_TYPE;
    }

    @Override
    public Set<StateKind> requires() {
        return Set.of(StateKind.POSITION);
    }

    @Override
    public Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule, TradeContext context) {
        PositionLimitParameters bands = PositionLimitParameters.from(rule.parameters());
        long net = context.position().netQuantity();
        long magnitude = Math.abs(net);

        Severity severity;
        if (magnitude > bands.criticalQuantity()) {
            severity = Severity.CRITICAL;
        } else if (magnitude > bands.warnQuantity()) {
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
                .setAlertType(AlertType.POSITION_LIMIT_BREACH)
                .setSeverity(severity)
                .setTicker(source.getTicker())
                .setTraderId(source.getTraderId())
                .setDescription("Net position of " + net + " shares in " + source.getTicker()
                        + " exceeds the governed position limit")
                .setRuleParameters(Map.of(
                        PositionLimitParameters.WARN_KEY, Long.toString(bands.warnQuantity()),
                        PositionLimitParameters.CRITICAL_KEY, Long.toString(bands.criticalQuantity())))
                .setMeasuredValues(Map.of(
                        "net-position-quantity", Long.toString(net),
                        "net-position-magnitude", Long.toString(magnitude),
                        "triggering-trade-quantity", Long.toString(source.getQuantity()),
                        "triggering-trade-side", source.getSide().toString()))
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
