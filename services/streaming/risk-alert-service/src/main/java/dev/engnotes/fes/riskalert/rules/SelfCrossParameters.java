package dev.engnotes.fes.riskalert.rules;

import java.util.Map;

/**
 * The governed window and tolerances of the self-cross rule.
 *
 * <p>Unbanded: {@link SelfCrossRule}'s Javadoc explains why severity is a constant here.
 *
 * <p>{@code windowSeconds} is bounded above by the store's configured candidate horizon,
 * {@code RiskAlertProperties.recentTradeHorizonSeconds()}, passed in as {@code maxWindowSeconds}. The
 * candidate query never looks further back than that horizon, so a governed window wider than it
 * would evaluate against a set the store has already truncated, and the rule would quietly
 * under-alert rather than fail loudly.
 *
 * <p>Nothing here defaults. A governed version whose parameters are incomplete or unparseable is
 * rejected outright and the previously in-force version stays in force (ADR-035). Defaulting a
 * missing band would make an approved rule evaluate against a number no approver ever saw.
 */
public record SelfCrossParameters(long windowSeconds, double quantityTolerancePercent, double priceTolerancePercent) {

    public static final String WINDOW_KEY = "window-seconds";
    public static final String QUANTITY_TOLERANCE_KEY = "quantity-tolerance-percent";
    public static final String PRICE_TOLERANCE_KEY = "price-tolerance-percent";

    public static SelfCrossParameters from(Map<String, String> parameters, long maxWindowSeconds) {
        long windowSeconds = requirePositiveWhole(WINDOW_KEY, parameters);
        double quantityTolerance = requirePositiveDouble(QUANTITY_TOLERANCE_KEY, parameters);
        double priceTolerance = requirePositiveDouble(PRICE_TOLERANCE_KEY, parameters);

        if (windowSeconds > maxWindowSeconds) {
            throw new InvalidRuleParametersException("window_exceeds_horizon",
                    WINDOW_KEY + " (" + windowSeconds + ") must not exceed the configured recent-trade "
                            + "horizon (" + maxWindowSeconds + "): the candidate query never looks "
                            + "further back than that horizon, so a wider governed window would "
                            + "evaluate against a truncated set and quietly under-alert.");
        }

        return new SelfCrossParameters(windowSeconds, quantityTolerance, priceTolerance);
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
                    "Parameter " + key + " is not a whole number of seconds: " + raw);
        }

        if (value <= 0) {
            throw new InvalidRuleParametersException("not_positive",
                    "Parameter " + key + " must be above zero: " + raw);
        }
        return value;
    }

    private static double requirePositiveDouble(String key, Map<String, String> parameters) {
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

        if (value <= 0) {
            throw new InvalidRuleParametersException("not_positive",
                    "Parameter " + key + " must be above zero: " + raw);
        }
        return value;
    }
}
