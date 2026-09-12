package dev.engnotes.fes.riskalert.window;

import java.math.BigDecimal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class VolumeWindowTest {

    @Test
    void mean_and_standard_deviation_come_from_the_additive_totals() {
        // Quantities 2, 4, 4, 4, 5, 5, 7, 9: mean 5, population standard deviation 2.
        VolumeWindow window = new VolumeWindow("RELIANCE", 8L,
                BigDecimal.valueOf(40L), BigDecimal.valueOf(232L));

        assertThat(window.mean()).isEqualByComparingTo("5");
        assertThat(window.standardDeviation()).isEqualByComparingTo("2");
    }

    @Test
    void a_single_sample_has_a_zero_standard_deviation_rather_than_a_failure() {
        VolumeWindow window = new VolumeWindow("RELIANCE", 1L,
                BigDecimal.valueOf(100L), BigDecimal.valueOf(10_000L));

        assertThat(window.standardDeviation()).isEqualByComparingTo("0");
    }

    @Test
    void an_empty_window_reports_zero_rather_than_dividing_by_zero() {
        VolumeWindow window = new VolumeWindow("RELIANCE", 0L, BigDecimal.ZERO, BigDecimal.ZERO);

        assertThat(window.mean()).isEqualByComparingTo("0");
        assertThat(window.standardDeviation()).isEqualByComparingTo("0");
    }

    @Test
    void large_quantities_do_not_cancel_to_a_negative_variance() {
        // Two trades of 1,000,000,000 shares each. In double arithmetic the sum of squares is
        // 2e18, past the exact-integer range, and (Sx2 - (Sx)^2/n) cancels to a small negative
        // value whose square root is NaN. In BigDecimal it is exactly zero.
        BigDecimal q = new BigDecimal("1000000000");
        VolumeWindow window = new VolumeWindow("RELIANCE", 2L,
                q.multiply(BigDecimal.valueOf(2L)), q.multiply(q).multiply(BigDecimal.valueOf(2L)));

        assertThat(window.mean()).isEqualByComparingTo("1000000000");
        assertThat(window.standardDeviation())
                .as("a NaN here reads as a rule that silently stopped alerting")
                .isEqualByComparingTo("0");
    }
}
