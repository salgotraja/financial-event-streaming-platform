package dev.engnotes.fes.riskalert.correlation;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

/**
 * The candidate prior trades backing {@code WASH_TRADE_DETECTED}, scoped to one
 * {@code (trader_id, ticker)} within a fixed {@code horizonSeconds} and bounded by
 * {@code candidateCap}.
 *
 * <p><strong>Candidates are bounded by arrival order, {@code applied_seq}, not by
 * {@code event_timestamp}.</strong> Event-time bounding diverges on replay: a trade at t=100
 * arriving after a trade at t=200 is invisible to the second trade's first delivery and visible to
 * its replay, so the verdict would change between the two. {@code applied_seq} is fixed at first
 * insert and {@code ON CONFLICT (trade_id) DO NOTHING} preserves it, so a redelivery sees exactly
 * the candidate set the first delivery saw. That is also why this table needs no pinned-match
 * column the way {@link dev.engnotes.fes.riskalert.position.RiskPositionStore} and
 * {@link dev.engnotes.fes.riskalert.window.RiskVolumeWindowStore} carry one: the candidate query
 * bounded by {@code applied_seq < this trade's own applied_seq} already returns the same rows on
 * every delivery, because those rows are keyed on an ordering that is fixed on first insert and
 * never mutated afterward.
 *
 * <p><strong>{@code BIGSERIAL} is safe here only under a single-writer-per-key assumption.</strong>
 * A sequence is non-transactional, so two concurrent transactions can in general commit out of
 * sequence order. Within one {@code (trader_id, ticker)} they cannot: {@code trades.enriched} is
 * keyed on ticker, so each key is single-writer within the consumer group, the same assumption
 * {@link dev.engnotes.fes.riskalert.position.RiskPositionStore}'s row-lock concurrency control
 * rests on.
 *
 * <p><strong>The prune horizon is the topic retention, not the configured window.</strong> A row is
 * only deleted once it is older than {@code trades.enriched}'s 7-day retention, because a record
 * that old can never be redelivered. Pruning at {@code horizonSeconds} instead would delete a row a
 * redelivery still needs to reproduce its original candidate set.
 */
public class RiskRecentTradeStore {

    private static final long RETENTION_SECONDS = Duration.ofDays(7).toSeconds();

    private final JdbcClient jdbc;
    private final long horizonSeconds;
    private final int candidateCap;

    public RiskRecentTradeStore(JdbcClient jdbc, long horizonSeconds, int candidateCap) {
        this.jdbc = jdbc;
        this.horizonSeconds = horizonSeconds;
        this.candidateCap = candidateCap;
    }

    @Transactional
    public RecentTrades apply(EnrichedTradeEvent event) {
        TradeEvent trade = event.getTrade();
        String tradeId = trade.getTradeId().toString();
        String traderId = trade.getTraderId().toString();
        String ticker = trade.getTicker().toString();
        Side side = trade.getSide();
        long quantity = trade.getQuantity();
        BigDecimal price = BigDecimal.valueOf(trade.getPrice());
        Instant eventTimestamp = trade.getEventTimestamp();

        List<Long> inserted = jdbc.sql("""
                        INSERT INTO risk_recent_trade
                            (trade_id, trader_id, ticker, side, quantity, price, event_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (trade_id) DO NOTHING
                        RETURNING applied_seq
                        """)
                .params(tradeId, traderId, ticker, side.name(), quantity, price,
                        Timestamp.from(eventTimestamp))
                .query(Long.class)
                .list();

        long appliedSeq = inserted.isEmpty()
                ? jdbc.sql("SELECT applied_seq FROM risk_recent_trade WHERE trade_id = ?")
                        .param(tradeId)
                        .query(Long.class)
                        .single()
                : inserted.get(0);

        // Records older than the trades.enriched retention can never be redelivered, so deleting
        // them cannot break replay determinism. Pruning at horizonSeconds instead would delete a
        // row a redelivery still needs.
        Instant retentionCutoff = eventTimestamp.minusSeconds(RETENTION_SECONDS);
        jdbc.sql("DELETE FROM risk_recent_trade WHERE trader_id = ? AND ticker = ? AND event_timestamp < ?")
                .params(traderId, ticker, Timestamp.from(retentionCutoff))
                .update();

        Instant windowStart = eventTimestamp.minusSeconds(horizonSeconds);
        Instant windowEnd = eventTimestamp.plusSeconds(horizonSeconds);

        List<RecentTrade> rows = jdbc.sql("""
                        SELECT trade_id, applied_seq, side, quantity, price, event_timestamp
                        FROM risk_recent_trade
                        WHERE trader_id = ? AND ticker = ?
                          AND applied_seq < ?
                          AND event_timestamp >= ?
                          AND event_timestamp <= ?
                        ORDER BY applied_seq DESC
                        LIMIT ?
                        """)
                .params(traderId, ticker, appliedSeq, Timestamp.from(windowStart),
                        Timestamp.from(windowEnd), candidateCap + 1)
                .query((rs, rowNum) -> new RecentTrade(
                        rs.getString("trade_id"),
                        rs.getLong("applied_seq"),
                        Side.valueOf(rs.getString("side")),
                        rs.getLong("quantity"),
                        rs.getBigDecimal("price").doubleValue(),
                        rs.getTimestamp("event_timestamp").toInstant()))
                .list();

        boolean truncated = rows.size() > candidateCap;
        List<RecentTrade> priorTrades = truncated
                ? new ArrayList<>(rows.subList(0, candidateCap))
                : rows;

        return new RecentTrades(appliedSeq, priorTrades, truncated);
    }
}
