package dev.engnotes.fes.riskalert.rules;

import java.util.Optional;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.governance.ActiveRule;

/**
 * One risk rule, evaluated against one trade under one governed rule version.
 *
 * <p><strong>This departs from specification-v1.2 deliberately (ADR-035).</strong> The specified
 * interface is a {@code boolean evaluate(...)} followed by a separate
 * {@code RiskAlertEvent buildAlert(...)}. Two calls compute the breach twice and can disagree with
 * each other, so a rule could report a breach and then build an alert describing no breach. One
 * call that returns the alert, or nothing, makes that impossible.
 */
public interface RiskRule {

    /**
     * The {@code ruleType} of {@code RiskRuleLifecycleEvent} this implementation serves. This is the
     * dispatch key, not {@code ruleId}: several governed rule ids may share one type.
     */
    String ruleType();

    Optional<RiskAlertEvent> evaluate(EnrichedTradeEvent trade, ActiveRule rule);
}
