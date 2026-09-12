package dev.engnotes.fes.riskalert.rules;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PositionLimitParametersTest {

    private static Map<String, String> bands(String warn, String critical) {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(PositionLimitParameters.WARN_KEY, warn);
        parameters.put(PositionLimitParameters.CRITICAL_KEY, critical);
        return parameters;
    }

    @Test
    void both_bands_are_parsed_when_present_and_ordered() {
        PositionLimitParameters parsed = PositionLimitParameters.from(bands("10000", "50000"));

        assertThat(parsed.warnQuantity()).isEqualTo(10_000L);
        assertThat(parsed.criticalQuantity()).isEqualTo(50_000L);
    }

    @Test
    void a_missing_band_is_rejected_rather_than_defaulted() {
        assertThatThrownBy(() -> PositionLimitParameters.from(
                Map.of(PositionLimitParameters.WARN_KEY, "10000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining(PositionLimitParameters.CRITICAL_KEY);
    }

    @Test
    void null_parameters_are_rejected() {
        assertThatThrownBy(() -> PositionLimitParameters.from(null))
                .isInstanceOf(InvalidRuleParametersException.class);
    }

    @Test
    void an_unparseable_band_is_rejected() {
        assertThatThrownBy(() -> PositionLimitParameters.from(bands("ten thousand", "50000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("not a whole number");
    }

    @Test
    void a_fractional_band_is_rejected_because_a_position_is_a_share_count() {
        assertThatThrownBy(() -> PositionLimitParameters.from(bands("10000.5", "50000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("not a whole number");
    }

    @Test
    void a_band_too_large_for_a_long_is_rejected_rather_than_truncated() {
        assertThatThrownBy(() -> PositionLimitParameters.from(bands("99999999999999999999", "50000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("not a whole number");
    }

    @Test
    void a_non_positive_band_is_rejected() {
        assertThatThrownBy(() -> PositionLimitParameters.from(bands("0", "50000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("above zero");
        assertThatThrownBy(() -> PositionLimitParameters.from(bands("-1", "50000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("above zero");
    }

    @Test
    void bands_out_of_order_are_rejected_so_severity_carries_information() {
        assertThatThrownBy(() -> PositionLimitParameters.from(bands("50000", "10000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("must be strictly above");
        assertThatThrownBy(() -> PositionLimitParameters.from(bands("10000", "10000")))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("must be strictly above");
    }
}
