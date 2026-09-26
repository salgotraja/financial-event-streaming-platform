package dev.engnotes.fes.riskalert;

import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.correlation.RecentTrade;
import dev.engnotes.fes.riskalert.correlation.RecentTrades;
import dev.engnotes.fes.riskalert.correlation.RiskRecentTradeStore;
import dev.engnotes.fes.riskalert.rules.EnrichedTrades;
import dev.engnotes.fes.testing.KafkaAvroStack;
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
class RiskRecentTradeStoreIntegrationTest {

    private static final String RULE_TOPIC = "ras-recentstore-rules-" + UUID.randomUUID();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        PostgresStack.start();
        RiskAlertTestKafka.createTopic(RULE_TOPIC, 6);
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.risk-alert-service.rule-topic", () -> RULE_TOPIC);
        registry.add("spring.datasource.url", PostgresStack::jdbcUrl);
        registry.add("spring.datasource.username", PostgresStack::username);
        registry.add("spring.datasource.password", PostgresStack::password);
    }

    @Autowired
    private DataSource dataSource;

    @Autowired
    private RiskRecentTradeStore store;

    private JdbcClient jdbc;

    @BeforeEach
    void clearState() {
        jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_recent_trade").update();
    }

    @Test
    void the_candidate_set_holds_the_traders_prior_trades_in_that_ticker() {
        store.apply(trade("t-1", "trader-1", "RELIANCE", Side.BUY, 100L, at("10:00:00")));
        store.apply(trade("t-2", "trader-1", "RELIANCE", Side.SELL, 100L, at("10:01:00")));
        RecentTrades candidates = store.apply(
                trade("t-3", "trader-1", "RELIANCE", Side.BUY, 100L, at("10:02:00")));

        assertThat(candidates.priorTrades())
                .extracting(RecentTrade::tradeId)
                .containsExactly("t-2", "t-1");
    }

    @Test
    void another_trader_and_another_ticker_are_not_candidates() {
        store.apply(trade("t-1", "trader-2", "RELIANCE", Side.BUY, 100L, at("10:00:00")));
        store.apply(trade("t-2", "trader-1", "INFY", Side.BUY, 100L, at("10:00:30")));
        RecentTrades candidates = store.apply(
                trade("t-3", "trader-1", "RELIANCE", Side.SELL, 100L, at("10:01:00")));

        assertThat(candidates.priorTrades()).isEmpty();
    }

    @Test
    void a_trade_is_never_its_own_candidate() {
        RecentTrades candidates = store.apply(
                trade("t-1", "trader-1", "TCS", Side.BUY, 100L, at("10:00:00")));

        assertThat(candidates.priorTrades()).isEmpty();
    }

    @Test
    void an_out_of_order_older_trade_applied_later_is_not_a_candidate_on_replay() {
        EnrichedTradeEvent later = trade("t-2", "trader-1", "WIPRO", Side.SELL, 100L, at("10:05:00"));
        RecentTrades first = store.apply(later);

        // t-1 has an OLDER event timestamp but a LATER arrival. Bounding by event time would make
        // it visible to the replay below and invisible to the first delivery, changing the verdict.
        store.apply(trade("t-1", "trader-1", "WIPRO", Side.BUY, 100L, at("10:01:00")));
        RecentTrades replayed = store.apply(later);

        assertThat(replayed.priorTrades()).isEmpty();
        assertThat(replayed.appliedSeq()).isEqualTo(first.appliedSeq());
    }

    @Test
    void a_prior_trade_later_in_event_time_but_earlier_in_arrival_order_is_still_a_candidate() {
        // t-1 arrives first, so its applied_seq is lower, but its event_timestamp is AFTER the
        // triggering trade's. The upper event_timestamp bound is what admits it: it measures the
        // horizon from the triggering trade's own timestamp forward, rather than treating that
        // timestamp as a ceiling, so an out-of-order arrival with a later event time still
        // qualifies as long as it falls inside the window. This is the counterpart to the replay
        // test above: that one pins the lower bound and arrival-order exclusion, this one pins the
        // upper bound's inclusion.
        store.apply(trade("t-1", "trader-1", "NESTLE", Side.BUY, 100L, at("10:30:00")));
        RecentTrades candidates = store.apply(
                trade("t-2", "trader-1", "NESTLE", Side.SELL, 100L, at("10:00:00")));

        assertThat(candidates.priorTrades())
                .extracting(RecentTrade::tradeId)
                .containsExactly("t-1");
    }

    @Test
    void a_redelivery_keeps_the_arrival_order_the_first_delivery_was_given() {
        EnrichedTradeEvent duplicate = trade("t-1", "trader-1", "HDFC", Side.BUY, 100L, at("10:00:00"));

        long first = store.apply(duplicate).appliedSeq();
        long second = store.apply(duplicate).appliedSeq();

        assertThat(second).isEqualTo(first);
    }

    @Test
    void trades_outside_the_configured_horizon_are_not_candidates() {
        store.apply(trade("t-1", "trader-1", "ITC", Side.BUY, 100L, at("08:00:00")));
        RecentTrades candidates = store.apply(
                trade("t-2", "trader-1", "ITC", Side.SELL, 100L, at("10:00:00")));

        // Two hours apart against a one-hour horizon.
        assertThat(candidates.priorTrades()).isEmpty();
    }

    @Test
    void hitting_the_candidate_cap_is_reported_rather_than_silently_truncated() {
        RiskRecentTradeStore capped = new RiskRecentTradeStore(jdbc, 3_600L, 2);
        capped.apply(trade("t-1", "trader-9", "SBIN", Side.BUY, 100L, at("10:00:00")));
        capped.apply(trade("t-2", "trader-9", "SBIN", Side.BUY, 100L, at("10:00:10")));
        capped.apply(trade("t-3", "trader-9", "SBIN", Side.BUY, 100L, at("10:00:20")));
        RecentTrades candidates = capped.apply(
                trade("t-4", "trader-9", "SBIN", Side.SELL, 100L, at("10:00:30")));

        assertThat(candidates.priorTrades()).hasSize(2);
        assertThat(candidates.truncated())
                .as("a cap that drops the offsetting trade must not be silent")
                .isTrue();
    }

    private static EnrichedTradeEvent trade(String tradeId, String traderId, String ticker, Side side,
                                            long quantity, Instant eventTimestamp) {
        return EnrichedTrades.withPosition(tradeId, traderId, ticker, side, quantity, eventTimestamp);
    }

    private static Instant at(String hhmmss) {
        return Instant.parse("2026-09-12T" + hhmmss + "Z");
    }
}
