package dev.engnotes.fes.tradeenrichment;

/**
 * Why a trade could not be enriched from reference and market data (ADR-027, ADR-034).
 *
 * <p>The label is the metric tag and appears in the dead-letter message, so the two cannot drift.
 */
public enum UnavailableReason {

    /** No tick has ever been projected for this ticker. */
    TICK_ABSENT("tick_absent"),
    /** The cached tick predates the trade by more than the configured maximum. */
    STALE("stale"),
    /** The cached tick postdates the trade, which replay produces and which must not be used. */
    FUTURE("future"),
    /** The rolling window carries no volume in the trade's horizon, so vwap5Min has no value. */
    WINDOW_EMPTY("window_empty"),
    /** The instrument master does not carry this ticker, so marketCap has no value. */
    INSTRUMENT_MISSING("instrument_missing");

    private final String label;

    UnavailableReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
