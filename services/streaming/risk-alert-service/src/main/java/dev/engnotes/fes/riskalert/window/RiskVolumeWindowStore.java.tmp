package dev.engnotes.fes.riskalert.window;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.TradeEvent;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

/**
 * The rolling per-ticker volume distribution backing {@code UNUSUAL_VOLUME}, over a fixed
 * {@code windowSeconds} horizon.
 *
 * <p><strong>The trade being evaluated is excluded from the distribution it is compared against.</strong>
 * {@code apply} folds the bucket totals after adding the trade, then subtracts its own
 * {@code (1, quantity, quantity * quantity)} contribution, so the caller always sees the prior
 * window rather than one that already contains the trade under test.
 *
 * <p><strong>A redelivery returns the historical window, not the current one.</strong> The ledger
 * row in {@code risk_volume_applied_trade} pins the distribution the first delivery evaluated
 * against, the same replay guarantee {@link dev.engnotes.fes.riskalert.position.RiskPositionStore}
 * gives its position (ADR-019, ADR-035).
 *
 * <p><strong>The per-ticker high-water mark only ever advances.</strong> Bucket pruning is driven by
 * this mark rather than by the current trade's own timestamp, because records arrive in offset
 * order but {@code eventTimestamp} can go backwards; taking the cutoff from a late trade would let
 * it resurrect a bucket a prior, in-order trade already pruned, changing every later fold for that
 * ticker.
 */
public class RiskVolumeWindowStore {

    private static final long BUCKET_WIDTH_SECONDS = 60L;

    private final JdbcClient jdbc;
    private final long windowSeconds;

    public RiskVolumeWindowStore(JdbcClient jdbc, long windowSeconds) {
        this.jdbc = jdbc;
        this.windowSeconds = windowSeconds;
    }

    @Transactional
    public VolumeWindow apply(EnrichedTradeEvent event) {
        TradeEvent trade = event.getTrade();
        String tradeId = trade.getTradeId().toString();
        String ticker = trade.getTicker().toString();
        long quantity = trade.getQuantity();
        Instant eventTimestamp = trade.getEventTimestamp();
        BigDecimal q = BigDecimal.valueOf(quantity);

        int claimed = jdbc.sql("""
                        INSERT INTO risk_volume_applied_trade
                            (trade_id, ticker, window_n, window_sum, window_sumsq, applied_at)
                        VALUES (?, ?, 0, 0, 0, ?)
                        ON CONFLICT (trade_id) DO NOTHING
                        """)
                .params(tradeId, ticker, Timestamp.from(Instant.now()))
                .update();

        if (claimed == 0) {
            // Already applied. The pinned totals are already the prior distribution: the
            // subtraction happened on the first delivery.
            return jdbc.sql("""
                            SELECT window_n, window_sum, window_sumsq
                            FROM risk_volume_applied_trade
                            WHERE trade_id = ?
                            """)
                    .param(tradeId)
                    .query((rs, rowNum) -> new VolumeWindow(ticker, rs.getLong("window_n"),
                            rs.getBigDecimal("window_sum"), rs.getBigDecimal("window_sumsq")))
                    .single();
        }

        long bucketStartEpochSecond = Math.floorDiv(eventTimestamp.getEpochSecond(), BUCKET_WIDTH_SECONDS)
                * BUCKET_WIDTH_SECONDS;
        Instant bucketStart = Instant.ofEpochSecond(bucketStartEpochSecond);

        Instant highWaterMark = jdbc.sql("""
                        INSERT INTO risk_volume_ticker (ticker, high_water_mark)
                        VALUES (?, ?)
                        ON CONFLICT (ticker) DO UPDATE SET
                            high_water_mark = GREATEST(risk_volume_ticker.high_water_mark, EXCLUDED.high_water_mark)
                        RETURNING high_water_mark
                        """)
                .params(ticker, Timestamp.from(eventTimestamp))
                .query(Instant.class)
                .single();

        jdbc.sql("""
                        INSERT INTO risk_volume_bucket
                            (ticker, bucket_start, trade_count, quantity_sum, quantity_sumsq)
                        VALUES (?, ?, 1, ?, ?)
                        ON CONFLICT (ticker, bucket_start) DO UPDATE SET
                            trade_count    = risk_volume_bucket.trade_count + EXCLUDED.trade_count,
                            quantity_sum   = risk_volume_bucket.quantity_sum + EXCLUDED.quantity_sum,
                            quantity_sumsq = risk_volume_bucket.quantity_sumsq + EXCLUDED.quantity_sumsq
                        """)
                .params(ticker, Timestamp.from(bucketStart), q, q.multiply(q))
                .update();

        Instant cutoff = highWaterMark.minusSeconds(windowSeconds);

        jdbc.sql("DELETE FROM risk_volume_bucket WHERE ticker = ? AND bucket_start < ?")
                .params(ticker, Timestamp.from(cutoff))
                .update();

        record Fold(long n, BigDecimal sum, BigDecimal sumSq) {
        }

        Fold fold = jdbc.sql("""
                        SELECT coalesce(sum(trade_count), 0) AS n,
                               coalesce(sum(quantity_sum), 0) AS s,
                               coalesce(sum(quantity_sumsq), 0) AS ssq
                        FROM risk_volume_bucket
                        WHERE ticker = ? AND bucket_start >= ?
                        """)
                .params(ticker, Timestamp.from(cutoff))
                .query((rs, rowNum) -> new Fold(rs.getLong("n"), rs.getBigDecimal("s"), rs.getBigDecimal("ssq")))
                .single();

        long foldedN = fold.n();
        BigDecimal foldedSum = fold.sum();
        BigDecimal foldedSumSq = fold.sumSq();

        // Subtract the trade's own contribution only if its own bucket survived the prune. A bucket
        // older than the cutoff was deleted in statement 4 before this fold ran, so the trade never
        // entered the fold and is already excluded; subtracting it again would drive the count below
        // the true prior count.
        if (!bucketStart.isBefore(cutoff)) {
            foldedN -= 1L;
            foldedSum = foldedSum.subtract(q);
            foldedSumSq = foldedSumSq.subtract(q.multiply(q));
        }
        if (foldedN < 0L) {
            foldedN = 0L;
        }

        jdbc.sql("""
                        UPDATE risk_volume_applied_trade
                        SET window_n = ?, window_sum = ?, window_sumsq = ?
                        WHERE trade_id = ?
                        """)
                .params(foldedN, foldedSum, foldedSumSq, tradeId)
                .update();

        return new VolumeWindow(ticker, foldedN, foldedSum, foldedSumSq);
    }
}
