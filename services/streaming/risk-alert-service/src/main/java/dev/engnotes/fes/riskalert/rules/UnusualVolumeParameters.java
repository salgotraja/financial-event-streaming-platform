package dev.engnotes.fes.riskalert.rules;

import java.util.Map;

/**
 * The governed sigma bands and minimum sample size of the unusual-volume rule.
 *
 * <p>Banded for the same reason {@link PositionLimitParameters} is: FR-04.3 requires a severity on
 * every alert, and one threshold makes severity a constant.
 *
 * <p>{@code minSampleCount} below two is rejected outright, because {@link
 * dev.engnotes.fes.riskalert.window.VolumeWindow#standardDeviation()} returns zero for fewer than
 * two samples, and a rule governed with a minimum of one would alert off a zero deviation.
 *
 * <p>Nothing here defaults. A governed version whose parameters are incomplete or unparseable is
 * rejected outright and the previously in-force version stays in force (ADR-035). Defaulting a
 * missing band would make an approved rule evaluate against a number no approver ever saw.
 */
public record UnusualVolumeParameters(double warnSigma, double criticalSigma, long minSampleCount) {

    public static final String WARN_KEY = "warn-sigma-multiplier";
    public static final String CRITICAL_KEY = "critical-sigma-multiplier";
    public static final String MIN_SAMPLES_KEY = "min-sample-count";

    public static UnusualVolumeParameters from(Map<String, String> parameters) {
        double warn = requirePositiveDouble(WARN_KEY, parameters);
        double critical = requirePositiveDouble(CRITICAL_KEY, parameters);
        long minSampleCount = requirePositiveWhole(MIN_SAMPLES_KEY, parameters);

        if (critical <= warn) {
            throw new InvalidRuleParametersException("bands_out_of_order",
                    CRITICAL_KEY + " (" + critical + ") must be strictly above " + WARN_KEY
                            + " (" + warn + "), otherwise every warning breach is also a critical one "
                            + "and the severity carries no information.");
        }

        if (minSampleCount < 2L) {
            throw new InvalidRuleParametersException("not_enough_samples",
                    MIN_SAMPLES_KEY + " (" + minSampleCount + ") must be at least 2: a standard "
                            + "deviation needs at least two samples, and a governed minimum below "
                            + "that would alert off a zero deviation.");
        }

        return new UnusualVolumeParameters(warn, critical, minSampleCount);
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
                    "Parameter " + key + " is not a whole number: " + raw);
        }

        if (value <= 0) {
            throw new InvalidRuleParametersException("not_positive",
                    "Parameter " + key + " must be above zero: " + raw);
        }
        return value;
    }
}
