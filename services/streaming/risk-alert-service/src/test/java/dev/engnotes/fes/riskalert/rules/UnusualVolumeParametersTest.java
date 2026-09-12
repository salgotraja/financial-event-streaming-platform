package dev.engnotes.fes.riskalert.rules;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class UnusualVolumeParametersTest {

    private static Map<String, String> bands(String warn, String critical, String minSamples) {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(UnusualVolumeParameters.WARN_KEY, warn);
        parameters.put(UnusualVolumeParameters.CRITICAL_KEY, critical);
        parameters.put(UnusualVolumeParameters.MIN_SAMPLES_KEY, minSamples);
        return parameters;
    }

    @Test
    void all_three_are_parsed_when_present_and_ordered() {
        UnusualVolumeParameters parsed = UnusualVolumeParameters.from(bands("3.0", "5.0", "30"));

        assertThat(parsed.warnSigma()).isEqualTo(3.0);
        assertThat(parsed.criticalSigma()).isEqualTo(5.0);
        assertThat(parsed.minSampleCount()).isEqualTo(30L);
    }

    @Test
    void a_missing_warn_sigma_is_rejected() {
        Map<String, String> parameters = bands("3.0", "5.0", "30");
        parameters.remove(UnusualVolumeParameters.WARN_KEY);

        assertThatThrownBy(() -> UnusualVolumeParameters.from(parameters))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("missing_parameter");
    }

    @Test
    void a_missing_critical_sigma_is_rejected() {
        Map<String, String> parameters = bands("3.0", "5.0", "30");
        parameters.remove(UnusualVolumeParameters.CRITICAL_KEY);

        assertThatThrownBy(() -> UnusualVolumeParameters.from(parameters))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("missing_parameter");
    }

    @Test
    void a_missing_min_sample_count_is_rejected() {
        Map<String, String> parameters = bands("3.0", "5.0", "30");
        parameters.remove(UnusualVolumeParameters.MIN_SAMPLES_KEY);

        assertThatThrownBy(() -> UnusualVolumeParameters.from(parameters))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("missing_parameter");
    }

    @Test
    void null_parameters_are_rejected() {
        assertThatThrownBy(() -> UnusualVolumeParameters.from(null))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("missing_parameter");
    }

    @Test
    void an_unparseable_sigma_is_rejected() {
        assertThatThrownBy(() -> UnusualVolumeParameters.from(bands("three", "5.0", "30")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("unparseable_value");
    }

    @Test
    void an_unparseable_min_sample_count_is_rejected() {
        assertThatThrownBy(() -> UnusualVolumeParameters.from(bands("3.0", "5.0", "thirty")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("unparseable_value");
    }

    @Test
    void a_non_positive_sigma_is_rejected() {
        assertThatThrownBy(() -> UnusualVolumeParameters.from(bands("0", "5.0", "30")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("not_positive");
        assertThatThrownBy(() -> UnusualVolumeParameters.from(bands("-1.0", "5.0", "30")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("not_positive");
    }

    @Test
    void a_non_positive_min_sample_count_is_rejected() {
        assertThatThrownBy(() -> UnusualVolumeParameters.from(bands("3.0", "5.0", "0")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("not_positive");
    }

    @Test
    void bands_out_of_order_are_rejected() {
        assertThatThrownBy(() -> UnusualVolumeParameters.from(bands("5.0", "3.0", "30")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("bands_out_of_order");
        assertThatThrownBy(() -> UnusualVolumeParameters.from(bands("3.0", "3.0", "30")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("bands_out_of_order");
    }

    @Test
    void a_minimum_sample_below_two_is_rejected() {
        assertThatThrownBy(() -> UnusualVolumeParameters.from(Map.of(
                UnusualVolumeParameters.WARN_KEY, "3.0",
                UnusualVolumeParameters.CRITICAL_KEY, "4.0",
                UnusualVolumeParameters.MIN_SAMPLES_KEY, "1")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("not_enough_samples");
    }
}
