package dev.engnotes.fes.positionexposure.position;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The running per-{@code (accountId, traderId, ticker)} position, applied once per trade and
 * idempotent by {@code tradeId} (FR-11.3).
 *
 * <p><strong>A redelivery answers from the pinned ledger row, not the current position.</strong>
 * Unlike {@code RiskPositionStore}, which pins only the net because a rule reads only the net, this
 * store's whole output is the snapshot: {@code position_applied_trade} pins all four published
 * figures, so a redelivery reproduces every one of them rather than recomputing any against a
 * position that later trades may since have moved.
 *
 * <p><strong>Concurrency control is the UPSERT's row lock, not optimistic locking</strong>, for the
 * same reason ADR-036 gives for {@code risk_position}: {@code trades.enriched} is keyed on ticker, so
 * each {@code (account_id, trader_id, ticker)} is single-writer within the consumer group and a
 * retry loop would never fire.
 *
 * <p><strong>{@code market_value} is assigned, never accumulated.</strong> It is the new net times
 * this trade's mid price, an instantaneous mark rather than a running sum, which would be
 * meaningless. It is also the one column where a late-arriving trade (an {@code eventTimestamp}
 * behind {@code last_event_timestamp}) can overwrite a newer valuation with a value derived from an
 * older trade: that is accepted, it is mark-to-last-*applied*-trade rather than mark-to-market, and
 * the alternative is carrying a second timestamp to guard one derived figure.
 *
 * <p>Not a Spring Data repository. Four hand-written statements, one of them an
 * {@code ON CONFLICT ... RETURNING} upsert, are clearer as {@link JdbcClient} calls than as a
 * {@code CrudRepository} plus {@code @Query} overrides that bypass it anyway.
 */
@Component
public class PositionStore {

    private final JdbcClient jdbc;

    public PositionStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Applies one trade to its account/trader/ticker position and returns the position immediately
     * after it. Calling this twice with the same {@code tradeId} applies it once and returns the
     * same answer both times.
     */
    @Transactional
    public Position apply(EnrichedTradeEvent enriched) {
        TradeEvent trade = enriched.getTrade();
        String tradeId = trade.getTradeId().toString();
        String accountId = trade.getAccountId().toString();
        String traderId = trade.getTraderId().toString();
        String ticker = trade.getTicker().toString();

        int claimed = jdbc.sql("""
                        INSERT INTO position_applied_trade
                            (trade_id, account_id, trader_id, ticker,
                             net_quantity_after, gross_buy_after, gross_sell_after, market_value_after,
                             applied_at)
                        VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
                        ON CONFLICT (trade_id) DO NOTHING
                        """)
                .params(tradeId, accountId, traderId, ticker, Timestamp.from(Instant.now()))
                .update();

        if (claimed == 0) {
            // Already applied. The pinned figures are what the first delivery produced.
            Optional<PinnedRow> pinned = jdbc.sql("""
                            SELECT net_quantity_after, gross_buy_after, gross_sell_after, market_value_after
                            FROM position_applied_trade WHERE trade_id = ?
                            """)
                    .param(tradeId)
                    .query((rs, rowNum) -> new PinnedRow(
                            rs.getObject("net_quantity_after", Long.class),
                            rs.getObject("gross_buy_after", Long.class),
                            rs.getObject("gross_sell_after", Long.class),
                            rs.getBigDecimal("market_value_after")))
                    .optional();

            PinnedRow row = pinned.orElseThrow();
            if (row.netQuantityAfter == null) {
                throw new IllegalStateException(
                        "Ledger row for tradeId=" + tradeId + " exists but carries no net_quantity_after");
            }
            return new Position(accountId, traderId, ticker, row.netQuantityAfter, row.grossBuyAfter,
                    row.grossSellAfter, row.marketValueAfter);
        }

        long signedDelta = trade.getSide() == Side.BUY ? trade.getQuantity() : -trade.getQuantity();
        long grossBuy = trade.getSide() == Side.BUY ? trade.getQuantity() : 0L;
        long grossSell = trade.getSide() == Side.SELL ? trade.getQuantity() : 0L;
        Instant eventTimestamp = trade.getEventTimestamp();
        Instant now = Instant.now();
        BigDecimal midPrice = BigDecimal.valueOf(enriched.getMidPriceAtExecution());

        // GREATEST rather than assignment on last_event_timestamp, for the reason RiskPositionStore
        // documents: records arrive in offset order but eventTimestamp can go backwards, and a sum
        // is order-independent where a maximum is not. market_value is assigned rather than
        // accumulated: it is the new net times this trade's mid price, so a late-arriving trade can
        // overwrite a newer valuation with an older mark. That is accepted as mark-to-last-applied-
        // trade rather than mark-to-market.
        PositionRow updated = jdbc.sql("""
                        INSERT INTO position
                            (account_id, trader_id, ticker, net_quantity, gross_buy_quantity,
                             gross_sell_quantity, market_value, last_event_timestamp, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (account_id, trader_id, ticker) DO UPDATE SET
                            net_quantity         = position.net_quantity + EXCLUDED.net_quantity,
                            gross_buy_quantity   = position.gross_buy_quantity + EXCLUDED.gross_buy_quantity,
                            gross_sell_quantity  = position.gross_sell_quantity + EXCLUDED.gross_sell_quantity,
                            market_value         = ? * (position.net_quantity + EXCLUDED.net_quantity),
                            last_event_timestamp = GREATEST(position.last_event_timestamp,
                                                            EXCLUDED.last_event_timestamp),
                            updated_at           = EXCLUDED.updated_at
                        RETURNING net_quantity, gross_buy_quantity, gross_sell_quantity, market_value
                        """)
                .params(accountId, traderId, ticker, signedDelta, grossBuy, grossSell,
                        midPrice.multiply(BigDecimal.valueOf(signedDelta)), Timestamp.from(eventTimestamp),
                        Timestamp.from(now), midPrice)
                .query((rs, rowNum) -> new PositionRow(
                        rs.getLong("net_quantity"), rs.getLong("gross_buy_quantity"),
                        rs.getLong("gross_sell_quantity"), rs.getBigDecimal("market_value")))
                .single();

        jdbc.sql("""
                        UPDATE position_applied_trade
                        SET net_quantity_after = ?, gross_buy_after = ?, gross_sell_after = ?,
                            market_value_after = ?
                        WHERE trade_id = ?
                        """)
                .params(updated.netQuantity, updated.grossBuy, updated.grossSell, updated.marketValue, tradeId)
                .update();

        return new Position(accountId, traderId, ticker, updated.netQuantity, updated.grossBuy,
                updated.grossSell, updated.marketValue);
    }

    private record PinnedRow(Long netQuantityAfter, Long grossBuyAfter, Long grossSellAfter,
                              BigDecimal marketValueAfter) {
    }

    private record PositionRow(long netQuantity, long grossBuy, long grossSell, BigDecimal marketValue) {
    }
}
