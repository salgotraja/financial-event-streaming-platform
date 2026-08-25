package dev.engnotes.fes.riskalert.rules;

import java.util.Map;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PriceDeviationParametersTest {

    @Test
    void both_bands_parse() {
        PriceDeviationParameters parameters = PriceDeviationParameters.from(Map.of(
                "warn-deviation-percent", "2.0",
                "critical-deviation-percent", "5.0"));

        assertThat(parameters.warnPercent()).isEqualTo(2.0);
        assertThat(parameters.criticalPercent()).isEqualTo(5.0);
    }

    @Test
    void a_missing_band_is_rejected() {
        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> PriceDeviationParameters.from(Map.of("warn-deviation-percent", "2.0")))
                .satisfies(e -> assertThat(e.reason()).isEqualTo("missing_parameter"))
                .withMessageContaining("critical-deviation-percent");
    }

    @Test
    void the_specifications_single_threshold_name_is_not_silently_accepted() {
        // specification-v1.2 names max-deviation-percent for an unbanded rule. Accepting it and
        // defaulting the other band would make a governed version mean something other than what it
        // says, which is the one thing a governance record must never do.
        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> PriceDeviationParameters.from(Map.of("max-deviation-percent", "2.0")));
    }

    @Test
    void an_unparseable_value_is_rejected() {
        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> PriceDeviationParameters.from(Map.of(
                        "warn-deviation-percent", "two percent",
                        "critical-deviation-percent", "5.0")))
                .satisfies(e -> assertThat(e.reason()).isEqualTo("unparseable_value"));
    }

    @Test
    void a_non_finite_value_is_rejected() {
        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> PriceDeviationParameters.from(Map.of(
                        "warn-deviation-percent", "NaN",
                        "critical-deviation-percent", "5.0")))
                .satisfies(e -> assertThat(e.reason()).isEqualTo("not_finite"));
    }

    @Test
    void a_non_positive_band_is_rejected() {
        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> PriceDeviationParameters.from(Map.of(
                        "warn-deviation-percent", "0.0",
                        "critical-deviation-percent", "5.0")))
                .satisfies(e -> assertThat(e.reason()).isEqualTo("not_positive"));
    }

    @Test
    void a_critical_band_below_the_warning_band_is_rejected() {
        // Otherwise every WARNING breach is also a CRITICAL one and the severity carries no
        // information at all.
        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> PriceDeviationParameters.from(Map.of(
                        "warn-deviation-percent", "5.0",
                        "critical-deviation-percent", "2.0")))
                .satisfies(e -> assertThat(e.reason()).isEqualTo("bands_out_of_order"));
    }

    @Test
    void equal_bands_are_rejected() {
        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> PriceDeviationParameters.from(Map.of(
                        "warn-deviation-percent", "2.0",
                        "critical-deviation-percent", "2.0")))
                .satisfies(e -> assertThat(e.reason()).isEqualTo("bands_out_of_order"));
    }
}
