package dev.engnotes.fes.tradeenrichment;

/**
 * The market state one trade was enriched against.
 *
 * <p>The window is carried as the two sums rather than as a computed average so the caller can tell
 * an empty window from a zero price: {@code windowVolume} of zero is the {@code window_empty} case
 * and must never reach a division.
 */
public record MarketSnapshot(long eventTimestampMillis,
                             double bidPrice,
                             double askPrice,
                             double lastTradedPrice,
                             double windowPriceVolume,
                             double windowVolume) {
}
