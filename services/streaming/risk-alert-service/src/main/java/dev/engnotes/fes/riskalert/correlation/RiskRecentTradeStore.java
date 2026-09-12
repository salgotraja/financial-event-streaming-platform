package dev.engnotes.fes.riskalert.correlation;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The candidate prior trades backing {@code WASH_TRADE_DETECTED}, scoped to one
 * {@code (trader_id, ticker)} within a fixed {@code horizonSeconds} and bounded by
 * {@code candidateCap}.
 *
 * <p>Placeholder for Task 2: the constructor carries its final shape so Tasks 3 and 5 change only
 * the method body, not every test that builds a {@code TradeStateStores}. {@link #apply} is filled
 * in by Task 5.
 */
public class RiskRecentTradeStore {

    private final JdbcClient jdbc;
    private final long horizonSeconds;
    private final int candidateCap;

    public RiskRecentTradeStore(JdbcClient jdbc, long horizonSeconds, int candidateCap) {
        this.jdbc = jdbc;
        this.horizonSeconds = horizonSeconds;
        this.candidateCap = candidateCap;
    }

    public RecentTrades apply(EnrichedTradeEvent event) {
        throw new UnsupportedOperationException("implemented in task 5");
    }
}
