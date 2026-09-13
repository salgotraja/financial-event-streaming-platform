package dev.engnotes.fes.positionexposure;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Map;
import javax.sql.DataSource;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.positionexposure.position.Position;
import dev.engnotes.fes.positionexposure.position.PositionStore;
import dev.engnotes.fes.testing.PostgresStack;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
class PositionStoreIntegrationTest {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresStack.start();
        registry.add("spring.datasource.url", PostgresStack::jdbcUrl);
        registry.add("spring.datasource.username", PostgresStack::username);
        registry.add("spring.datasource.password", PostgresStack::password);
    }

    @Autowired
    private PositionStore store;

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void clearState() {
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE position_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE position").update();
    }

    @Test
    void a_buy_and_a_sell_net_against_each_other_and_both_add_to_gross() {
        store.apply(trade("t-1", "acc-1", "trader-1", "RELIANCE", Side.BUY, 100L, 2500.0, at("10:00:00")));
        Position position = store.apply(
                trade("t-2", "acc-1", "trader-1", "RELIANCE", Side.SELL, 40L, 2500.0, at("10:01:00")));

        assertThat(position.netQuantity()).isEqualTo(60L);
        assertThat(position.grossBuyQuantity()).isEqualTo(100L);
        assertThat(position.grossSellQuantity()).isEqualTo(40L);
    }

    @Test
    void a_short_position_is_signed_rather_than_clamped() {
        Position position = store.apply(
                trade("t-1", "acc-1", "trader-1", "INFY", Side.SELL, 100L, 1500.0, at("10:00:00")));

        assertThat(position.netQuantity()).isEqualTo(-100L);
        assertThat(position.marketValue()).isEqualByComparingTo("-150000");
    }

    @Test
    void market_value_is_the_net_at_the_mid_price_of_the_trade_that_moved_it() {
        store.apply(trade("t-1", "acc-1", "trader-1", "TCS", Side.BUY, 100L, 3000.0, at("10:00:00")));
        // The second trade carries a different mid price, and it is that price the whole position
        // is valued at: mark-to-last-trade, not an average and not a live mark.
        Position position = store.apply(
                trade("t-2", "acc-1", "trader-1", "TCS", Side.BUY, 100L, 3200.0, at("10:01:00")));

        assertThat(position.netQuantity()).isEqualTo(200L);
        assertThat(position.marketValue()).isEqualByComparingTo("640000");
    }

    @Test
    void the_grain_separates_two_accounts_trading_one_ticker() {
        store.apply(trade("t-1", "acc-1", "trader-1", "WIPRO", Side.BUY, 100L, 400.0, at("10:00:00")));
        Position other = store.apply(
                trade("t-2", "acc-2", "trader-1", "WIPRO", Side.BUY, 30L, 400.0, at("10:01:00")));

        assertThat(other.netQuantity())
                .as("a second account is a second position, not an addition to the first")
                .isEqualTo(30L);
    }

    @Test
    void the_grain_separates_one_account_trading_two_tickers() {
        store.apply(trade("t-1", "acc-1", "trader-1", "HDFC", Side.BUY, 100L, 1600.0, at("10:00:00")));
        Position other = store.apply(
                trade("t-2", "acc-1", "trader-1", "ITC", Side.BUY, 70L, 450.0, at("10:01:00")));

        assertThat(other.netQuantity()).isEqualTo(70L);
        assertThat(other.ticker()).isEqualTo("ITC");
    }

    @Test
    void a_redelivery_reproduces_every_figure_the_first_delivery_produced() {
        EnrichedTradeEvent second =
                trade("t-2", "acc-1", "trader-1", "SBIN", Side.BUY, 50L, 600.0, at("10:01:00"));
        store.apply(trade("t-1", "acc-1", "trader-1", "SBIN", Side.BUY, 100L, 600.0, at("10:00:00")));

        Position first = store.apply(second);
        store.apply(trade("t-3", "acc-1", "trader-1", "SBIN", Side.BUY, 999L, 700.0, at("10:02:00")));
        Position replayed = store.apply(second);

        // t-3 moved the live position in between. The redelivery must answer from the pinned
        // ledger row, or the snapshot it republishes differs from the one it first published.
        assertThat(replayed).isEqualTo(first);
    }

    @Test
    void a_trade_applied_twice_moves_the_position_once() {
        EnrichedTradeEvent duplicate =
                trade("t-1", "acc-1", "trader-1", "BAJAJ", Side.BUY, 100L, 900.0, at("10:00:00"));
        store.apply(duplicate);
        store.apply(duplicate);

        Position position = store.apply(
                trade("t-2", "acc-1", "trader-1", "BAJAJ", Side.BUY, 1L, 900.0, at("10:01:00")));

        assertThat(position.netQuantity()).isEqualTo(101L);
    }

    private static Instant at(String time) {
        return LocalTime.parse(time).atDate(java.time.LocalDate.of(2026, 9, 13))
                .atZone(java.time.ZoneOffset.UTC).toInstant();
    }

    private static EnrichedTradeEvent trade(String tradeId,
                                      String accountId,
                                      String traderId,
                                      String ticker,
                                      Side side,
                                      long quantity,
                                      double midPrice,
                                      Instant eventTimestamp) {
        TradeEvent tradeEvent = TradeEvent.newBuilder()
                .setTradeId(tradeId)
                .setCorrelationId("corr-" + tradeId)
                .setTicker(ticker)
                .setQuantity(quantity)
                .setPrice(midPrice)
                .setSide(side)
                .setTraderId(traderId)
                .setAccountId(accountId)
                .setEventTimestamp(eventTimestamp)
                .setProducedAt(eventTimestamp.plusMillis(1L))
                .setTraceContext(Map.of("traceparent",
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
                .build();

        return EnrichedTradeEvent.newBuilder()
                .setTrade(tradeEvent)
                .setMidPriceAtExecution(midPrice)
                .setSpreadAtExecution(0.5)
                .setVwap5Min(midPrice)
                .setMarketCap(1_700_000.0)
                .setPriceDeviation(0.0)
                .setEnrichedAt(eventTimestamp.plusMillis(2L))
                .setEnrichmentLatencyMs(1L)
                .setMarketDataAgeMs(50L)
                .build();
    }
}
