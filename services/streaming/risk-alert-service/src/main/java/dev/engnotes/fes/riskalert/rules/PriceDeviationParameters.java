package dev.engnotes.fes.riskalert.rules;

import java.util.Map;

/**
 * The two banded thresholds of the price-deviation rule, in percent.
 *
 * <p>FR-04.2 names one 2 percent threshold and FR-04.3 requires a severity on every alert. One
 * threshold makes severity a constant and leaves the architecture's severity routing nothing to
 * route, so the rule is banded and both bands are required.
 *
 * <p>Nothing here defaults. A governed version whose parameters are incomplete or unparseable is
 * rejected outright and the previously in-force version stays in force (ADR-035). Defaulting a
 * missing band would make an approved rule evaluate against a number no approver ever saw.
 */
public record PriceDeviationParameters(double warnPercent, double criticalPercent) {

    public static final String WARN_KEY = "warn-deviation-percent";
    public static final String CRITICAL_KEY = "critical-deviation-percent";

    public static PriceDeviationParameters from(Map<String, String> parameters) {
        double warn = requirePositiveFinite(WARN_KEY, parameters);
        double critical = requirePositiveFinite(CRITICAL_KEY, parameters);

        if (critical <= warn) {
            throw new InvalidRuleParametersException("bands_out_of_order",
                    CRITICAL_KEY + " (" + critical + ") must be strictly above " + WARN_KEY
                            + " (" + warn + "), otherwise every warning breach is also a critical one "
                            + "and the severity carries no information.");
        }
        return new PriceDeviationParameters(warn, critical);
    }

    private static double requirePositiveFinite(String key, Map<String, String> parameters) {
        String raw = parameters == null ? null : parameters.get(key);
        if (raw == null) {
            throw new InvalidRuleParametersException("missing_parameter",
                    "Required parameter " + key + " is absent");
        }

        double value;
        try {
            value = Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            throw new InvalidRuleParametersException("unparseable_value",
                    "Parameter " + key + " is not a number: " + raw);
        }

        if (!Double.isFinite(value)) {
            throw new InvalidRuleParametersException("not_finite",
                    "Parameter " + key + " is not finite: " + raw);
        }
        if (value <= 0) {
            throw new InvalidRuleParametersException("not_positive",
                    "Parameter " + key + " must be above zero: " + raw);
        }
        return value;
    }
}
