package dev.engnotes.fes.tradeenrichment;

/**
 * The trade is well-formed but cannot be enriched, so it takes the bounded-retry then DLQ path
 * (ADR-027).
 *
 * <p>This is deliberately not a dependency failure. A Redis read that succeeds and returns an old
 * value is not a failing call, so it must not pause the container the way an outage does. The cost is
 * that a stalled market-data feed dead-letters the trade flow for as long as the stall lasts, which
 * is why {@code enrichment_reference_unavailable_total} exists to make it visible.
 */
public class ReferenceDataUnavailableException extends RuntimeException {

    private final UnavailableReason reason;

    public ReferenceDataUnavailableException(UnavailableReason reason, String message) {
        super("REFERENCE_DATA_UNAVAILABLE [" + reason.label() + "] " + message);
        this.reason = reason;
    }

    public UnavailableReason reason() {
        return reason;
    }
}
