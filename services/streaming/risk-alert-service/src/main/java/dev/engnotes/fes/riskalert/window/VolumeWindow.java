package dev.engnotes.fes.riskalert.window;

import java.math.BigDecimal;

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
}
