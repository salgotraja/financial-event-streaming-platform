package dev.engnotes.fes.common.cache;

/**
 * The market cache's key format and window geometry, shared because two services must agree on it.
 *
 * <p>{@code market-data-cache-projector} writes these keys and {@code trade-enrichment-service}
 * reads them. Service modules may not depend on each other (ADR-028), so without this class the
 * format and the bucket size would be duplicated in both with nothing but a comment holding them
 * together, and a divergence would surface as a quietly wrong VWAP rather than as a failure.
 *
 * <p>This is a naming and layout contract, the same kind of thing as a topic name. It computes no
 * financial value and makes no decision, so it does not breach the rule that platform-common holds
 * no business logic.
 *
 * <p>Two of the projector's constants deliberately stay in the projector. {@code MAX_FUTURE_SKEW} is
 * a write-side sanity bound no reader can observe. {@code WINDOW_TTL_SECONDS} is the one that looks
 * like it belongs here and does not: 600 is only how long an idle key survives before Redis reclaims
 * it, twice the horizon so an idling window is not evicted the moment it stops being written, and no
 * reader computes anything from it. {@code WINDOW_SECONDS} is different in kind, because the writer's
 * prune and the reader's fold must use the same number or the window holds a different span than the
 * reader assumes.
 *
 * <p>The braced segment is a Redis Cluster hash tag, so both keys occupy one slot and a single script
 * may touch both (ADR-033).
 */
public final class MarketCacheKeys {

    /** Width of one window bucket, in seconds. */
    public static final int BUCKET_SECONDS = 10;

    /** The rolling window's horizon, in seconds. */
    public static final int WINDOW_SECONDS = 300;

    private MarketCacheKeys() {
    }

    public static String tickKey(String ticker) {
        return "market:{" + ticker + "}:tick";
    }

    public static String windowKey(String ticker) {
        return "market:{" + ticker + "}:window";
    }

    /**
     * The epoch second at which this timestamp's bucket starts, which is also the numeric prefix of
     * its {@code :pv} and {@code :v} field names.
     *
     * <p>{@link Math#floorDiv} rather than {@code /}, twice. {@code project-tick.lua} uses
     * {@code math.floor}, which rounds towards negative infinity, and Java's integer division
     * truncates towards zero. The two agree for every timestamp after 1970 and disagree before it,
     * and a disagreement here would be a wrong answer rather than an error.
     */
    public static long bucketFor(long epochMillis) {
        long epochSecond = Math.floorDiv(epochMillis, 1_000L);
        return Math.floorDiv(epochSecond, BUCKET_SECONDS) * BUCKET_SECONDS;
    }
}
