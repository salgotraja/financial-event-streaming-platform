package dev.engnotes.fes.riskalert.position;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

/**
 * The running per-{@code (traderId, ticker)} position, applied once per trade and idempotent by
 * {@code tradeId} (FR-11.3).
 *
 * <p><strong>A redelivery returns the historical net, not the current one.</strong> The ledger row
 * pins {@code net_quantity_after}, so the second delivery of a trade re-evaluates against the
 * position that trade produced rather than the position as it stands now. Without that, replay under
 * at-least-once (ADR-019) could turn a breach into a non-breach or the reverse, and ADR-035's
 * promise that a replayed trade reproduces its original verdict would not survive a stateful rule.
 *
 * <p><strong>Concurrency control is the UPSERT's row lock, not optimistic locking.</strong>
 * {@code .claude/rules/database.md} asks for both optimistic locking on a running total (ADR-008)
 * and a unique constraint with {@code ON CONFLICT} over a read-then-write that races. For this row
 * those collide, and ADR-036 resolves it: the row lock is the control, and {@code version} is
 * incremented for the audit intent rather than as a compare-and-swap. {@code trades.enriched} is
 * keyed on ticker, so each {@code (trader_id, ticker)} is single-writer within the consumer group
 * and a retry loop would never fire.
 *
 * <p>Not a Spring Data repository. Three hand-written statements, one of them an
 * {@code ON CONFLICT ... RETURNING} upsert, are clearer as {@link JdbcClient} calls than as a
 * {@code CrudRepository} plus {@code @Query} overrides that bypass it anyway.
 */
public class RiskPositionStore {

    private final JdbcClient jdbc;

    public RiskPositionStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Applies one trade to its trader's position and returns the position immediately after it.
     * Calling this twice with the same {@code tradeId} applies it once and returns the same answer
     * both times.
     */
    @Transactional
    public NetPosition apply(EnrichedTradeEvent event) {
        TradeEvent trade = event.getTrade();
        String tradeId = trade.getTradeId().toString();
        String traderId = trade.getTraderId().toString();
        String ticker = trade.getTicker().toString();

        int claimed = jdbc.sql("""
                        INSERT INTO risk_position_applied_trade
                            (trade_id, trader_id, ticker, net_quantity_after, applied_at)
                        VALUES (?, ?, ?, NULL, ?)
                        ON CONFLICT (trade_id) DO NOTHING
                        """)
                .params(tradeId, traderId, ticker, Timestamp.from(Instant.now()))
                .update();

        if (claimed == 0) {
            // Already applied. The pinned net is what the first delivery evaluated against.
            Optional<Long> pinned = jdbc.sql(
                            "SELECT net_quantity_after FROM risk_position_applied_trade WHERE trade_id = ?")
                    .param(tradeId)
                    .query(Long.class)
                    .optional();
            return new NetPosition(traderId, ticker, pinned.orElseThrow(() -> new IllegalStateException(
                    "Ledger row for tradeId=" + tradeId + " exists but carries no net_quantity_after")));
        }

        long signedDelta = trade.getSide() == Side.BUY ? trade.getQuantity() : -trade.getQuantity();
        long grossBuy = trade.getSide() == Side.BUY ? trade.getQuantity() : 0L;
        long grossSell = trade.getSide() == Side.SELL ? trade.getQuantity() : 0L;
        Instant eventTimestamp = trade.getEventTimestamp();
        Instant now = Instant.now();

        // GREATEST rather than assignment: records arrive in offset order within a partition, but
        // eventTimestamp can go backwards. A sum is order-independent, a maximum is not.
        long net = jdbc.sql("""
                        INSERT INTO risk_position
                            (trader_id, ticker, net_quantity, gross_buy, gross_sell,
                             last_event_timestamp, version, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                        ON CONFLICT (trader_id, ticker) DO UPDATE SET
                            net_quantity         = risk_position.net_quantity + EXCLUDED.net_quantity,
                            gross_buy            = risk_position.gross_buy + EXCLUDED.gross_buy,
                            gross_sell           = risk_position.gross_sell + EXCLUDED.gross_sell,
                            last_event_timestamp = GREATEST(risk_position.last_event_timestamp,
                                                            EXCLUDED.last_event_timestamp),
                            version              = risk_position.version + 1,
                            updated_at           = EXCLUDED.updated_at
                        RETURNING net_quantity
                        """)
                .params(traderId, ticker, signedDelta, grossBuy, grossSell,
                        Timestamp.from(eventTimestamp), Timestamp.from(now))
                .query(Long.class)
                .single();

        jdbc.sql("UPDATE risk_position_applied_trade SET net_quantity_after = ? WHERE trade_id = ?")
                .params(net, tradeId)
                .update();

        return new NetPosition(traderId, ticker, net);
    }
}
