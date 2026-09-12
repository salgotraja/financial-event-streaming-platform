package dev.engnotes.fes.riskalert.rules;

import java.util.Map;

/**
 * The two banded thresholds of the position-limit rule, in shares.
 *
 * <p>Banded for the same reason {@link PriceDeviationParameters} is: FR-04.3 requires a severity on
 * every alert, and one threshold makes severity a constant.
 *
 * <p>Whole shares, not a double. A position is a count, {@code TradeEvent.quantity} is a
 * {@code long}, and a fractional bound would compare a long against a value it can never equal.
 *
 * <p>Nothing here defaults. A governed version whose parameters are incomplete or unparseable is
 * rejected outright and the previously in-force version stays in force (ADR-035). Defaulting a
 * missing band would make an approved rule evaluate against a number no approver ever saw.
 */
public record PositionLimitParameters(long warnQuantity, long criticalQuantity) {

    public static final String WARN_KEY = "warn-position-quantity";
    public static final String CRITICAL_KEY = "critical-position-quantity";

    public static PositionLimitParameters from(Map<String, String> parameters) {
        long warn = requirePositiveWhole(WARN_KEY, parameters);
        long critical = requirePositiveWhole(CRITICAL_KEY, parameters);

        if (critical <= warn) {
            throw new InvalidRuleParametersException("bands_out_of_order",
                    CRITICAL_KEY + " (" + critical + ") must be strictly above " + WARN_KEY
                            + " (" + warn + "), otherwise every warning breach is also a critical one "
                            + "and the severity carries no information.");
        }
        return new PositionLimitParameters(warn, critical);
    }

    private static long requirePositiveWhole(String key, Map<String, String> parameters) {
        String raw = parameters == null ? null : parameters.get(key);
        if (raw == null) {
            throw new InvalidRuleParametersException("missing_parameter",
                    "Required parameter " + key + " is absent");
        }

        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new InvalidRuleParametersException("unparseable_value",
                    "Parameter " + key + " is not a whole number of shares: " + raw);
        }

        if (value <= 0) {
            throw new InvalidRuleParametersException("not_positive",
                    "Parameter " + key + " must be above zero: " + raw);
        }
        return value;
    }
}
