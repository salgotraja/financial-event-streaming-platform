package dev.engnotes.fes.riskalert.rules;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SelfCrossParametersTest {

    private static final long HORIZON = 3_600L;

    private static Map<String, String> parameters(String window, String quantityTolerance, String priceTolerance) {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(SelfCrossParameters.WINDOW_KEY, window);
        parameters.put(SelfCrossParameters.QUANTITY_TOLERANCE_KEY, quantityTolerance);
        parameters.put(SelfCrossParameters.PRICE_TOLERANCE_KEY, priceTolerance);
        return parameters;
    }

    @Test
    void all_three_parameters_are_parsed_when_present() {
        SelfCrossParameters parsed = SelfCrossParameters.from(parameters("300", "1.0", "0.5"), HORIZON);

        assertThat(parsed.windowSeconds()).isEqualTo(300L);
        assertThat(parsed.quantityTolerancePercent()).isEqualTo(1.0);
        assertThat(parsed.priceTolerancePercent()).isEqualTo(0.5);
    }

    @Test
    void a_missing_parameter_is_rejected_rather_than_defaulted() {
        Map<String, String> parameters = parameters("300", "1.0", "0.5");
        parameters.remove(SelfCrossParameters.PRICE_TOLERANCE_KEY);

        assertThatThrownBy(() -> SelfCrossParameters.from(parameters, HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining(SelfCrossParameters.PRICE_TOLERANCE_KEY);
    }

    @Test
    void null_parameters_are_rejected() {
        assertThatThrownBy(() -> SelfCrossParameters.from(null, HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class);
    }

    @Test
    void an_unparseable_window_is_rejected() {
        assertThatThrownBy(() -> SelfCrossParameters.from(parameters("soon", "1.0", "0.5"), HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("not a whole number");
    }

    @Test
    void an_unparseable_tolerance_is_rejected() {
        assertThatThrownBy(() -> SelfCrossParameters.from(parameters("300", "one", "0.5"), HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("not a number");
    }

    @Test
    void a_non_positive_window_is_rejected() {
        assertThatThrownBy(() -> SelfCrossParameters.from(parameters("0", "1.0", "0.5"), HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("above zero");
        assertThatThrownBy(() -> SelfCrossParameters.from(parameters("-1", "1.0", "0.5"), HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("above zero");
    }

    @Test
    void a_non_positive_tolerance_is_rejected() {
        assertThatThrownBy(() -> SelfCrossParameters.from(parameters("300", "0", "0.5"), HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("above zero");
        assertThatThrownBy(() -> SelfCrossParameters.from(parameters("300", "1.0", "-0.5"), HORIZON))
                .isInstanceOf(InvalidRuleParametersException.class)
                .hasMessageContaining("above zero");
    }

    @Test
    void a_governed_window_wider_than_the_configured_horizon_is_rejected() {
        // The candidate query never looks further back than the configured horizon, so a wider
        // governed window would evaluate against a truncated set and quietly under-alert.
        assertThatThrownBy(() -> SelfCrossParameters.from(Map.of(
                SelfCrossParameters.WINDOW_KEY, "7200",
                SelfCrossParameters.QUANTITY_TOLERANCE_KEY, "1.0",
                SelfCrossParameters.PRICE_TOLERANCE_KEY, "1.0"), 3_600L))
                .isInstanceOf(InvalidRuleParametersException.class)
                .extracting(e -> ((InvalidRuleParametersException) e).reason())
                .isEqualTo("window_exceeds_horizon");
    }

    @Test
    void a_window_equal_to_the_configured_horizon_is_accepted() {
        assertThat(SelfCrossParameters.from(Map.of(
                SelfCrossParameters.WINDOW_KEY, "3600",
                SelfCrossParameters.QUANTITY_TOLERANCE_KEY, "1.0",
                SelfCrossParameters.PRICE_TOLERANCE_KEY, "1.0"), 3_600L).windowSeconds())
                .isEqualTo(3_600L);
    }
}
