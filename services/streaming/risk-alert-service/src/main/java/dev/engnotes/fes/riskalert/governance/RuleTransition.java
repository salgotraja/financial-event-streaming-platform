package dev.engnotes.fes.riskalert.governance;

import java.util.Map;

import dev.engnotes.fes.events.RiskRuleLifecycleEvent;
import dev.engnotes.fes.events.RuleState;

/**
 * One {@link RiskRuleLifecycleEvent} reduced to what version selection needs.
 *
 * <p>{@code inForceFrom} is the event's {@code effectiveAt}, or its {@code eventTimestamp} when
 * {@code effectiveAt} is null (FR-12.3). Both are event time. Nothing in selection reads a wall
 * clock, which is what makes replay reproduce the original verdict and makes the derived alertId
 * stable across a governance change.
 */
public record RuleTransition(String ruleId,
                             String ruleType,
                             long version,
                             RuleState state,
                             Map<String, String> parameters,
                             long inForceFrom) {

    public static RuleTransition of(RiskRuleLifecycleEvent event) {
        long inForceFrom = event.getEffectiveAt() != null
                ? event.getEffectiveAt().toEpochMilli()
                : event.getEventTimestamp().toEpochMilli();

        return new RuleTransition(
                event.getRuleId(),
                event.getRuleType(),
                event.getVersion(),
                event.getState(),
                Map.copyOf(event.getParameters()),
                inForceFrom);
    }
}
