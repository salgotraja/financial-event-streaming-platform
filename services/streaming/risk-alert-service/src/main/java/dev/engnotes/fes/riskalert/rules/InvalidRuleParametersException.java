package dev.engnotes.fes.riskalert.rules;

/**
 * A governed rule version carried parameters this code cannot use.
 *
 * <p>{@link #reason()} is a short stable slug rather than free text, because it becomes a metric
 * tag on {@code risk_rule_versions_rejected_total}. Free text would make that metric unbounded in
 * cardinality.
 */
public class InvalidRuleParametersException extends RuntimeException {

    private final String reason;

    public InvalidRuleParametersException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
