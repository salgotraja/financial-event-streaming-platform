package dev.engnotes.fes.riskalert.window;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * The distribution of trade quantities for one ticker over the rolling window, as it stood
 * immediately before one trade was applied.
 *
 * <p>The triggering trade is excluded from its own distribution. Including it makes the first trade
 * in a window compare against a sample of one, where the standard deviation is zero and every value
 * reads as infinitely anomalous. Task 3 does the exclusion in the store, so every reader of this
 * record sees the prior distribution and cannot forget.
 */
public record VolumeWindow(String ticker, long sampleCount, BigDecimal sum, BigDecimal sumOfSquares) {

    private static final MathContext PRECISION = new MathContext(34, RoundingMode.HALF_EVEN);

    public BigDecimal mean() {
        return sampleCount == 0L
                ? BigDecimal.ZERO
                : sum.divide(BigDecimal.valueOf(sampleCount), PRECISION);
    }

    /**
     * The population standard deviation of the trade quantities in the window.
     *
     * <p>Computed in {@link BigDecimal} rather than {@code double} deliberately. An hour of a busy
     * ticker can carry {@code sumOfSquares} past the exact-integer range of a double, where
     * {@code (Sx2 - (Sx)^2/n)} cancels to a small negative value and {@code Math.sqrt} returns
     * {@code NaN}. A NaN here does not fail: every comparison against it is false, so the rule
     * stops alerting and says nothing about it.
     *
     * <p>The variance is clamped at zero. Exact arithmetic cannot make it negative, so the clamp
     * defends only against a future change of representation, and costs one comparison.
     */
    public BigDecimal standardDeviation() {
        if (sampleCount < 2L) {
            return BigDecimal.ZERO;
        }
        BigDecimal n = BigDecimal.valueOf(sampleCount);
        BigDecimal variance = sumOfSquares.subtract(sum.multiply(sum).divide(n, PRECISION))
                .divide(n, PRECISION);
        return variance.signum() <= 0 ? BigDecimal.ZERO : variance.sqrt(PRECISION);
    }
}
