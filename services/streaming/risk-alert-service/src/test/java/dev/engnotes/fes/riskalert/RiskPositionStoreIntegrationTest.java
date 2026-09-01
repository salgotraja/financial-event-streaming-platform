package dev.engnotes.fes.riskalert;

import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.position.NetPosition;
import dev.engnotes.fes.riskalert.position.RiskPositionStore;
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
class RiskPositionStoreIntegrationTest {

    private static final String RULE_TOPIC = "ras-store-rules-" + UUID.randomUUID();

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
    private RiskPositionStore store;

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void clearState() {
        // Truncate, not an unqualified delete: this is a container-local schema created for this
        // run, and each test's arithmetic starts from an empty position.
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_position_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_position").update();
    }

    @Test
    void a_buy_adds_to_the_net_position_and_a_sell_subtracts_from_it() {
        store.apply(trade("t-1", Side.BUY, 400L, 1_000L));
        NetPosition afterSell = store.apply(trade("t-2", Side.SELL, 150L, 2_000L));

        assertThat(afterSell.netQuantity()).isEqualTo(250L);
        assertThat(afterSell.traderId()).isEqualTo("trader-1");
        assertThat(afterSell.ticker()).isEqualTo("RELIANCE");
    }

    @Test
    void applying_the_same_trade_id_twice_does_not_double_count() {
        store.apply(trade("t-1", Side.BUY, 400L, 1_000L));
        NetPosition second = store.apply(trade("t-1", Side.BUY, 400L, 1_000L));

        assertThat(second.netQuantity())
                .as("FR-11.3: reprocessing an event must not double-count a position")
                .isEqualTo(400L);
        assertThat(storedNet()).isEqualTo(400L);
    }

    @Test
    void a_redelivery_returns_the_historical_net_not_the_current_one() {
        store.apply(trade("t-1", Side.BUY, 400L, 1_000L));
        store.apply(trade("t-2", Side.BUY, 600L, 2_000L));

        NetPosition redelivered = store.apply(trade("t-1", Side.BUY, 400L, 1_000L));

        assertThat(redelivered.netQuantity())
                .as("the ledger pins the net that t-1 produced, so replay reproduces the original verdict")
                .isEqualTo(400L);
        assertThat(storedNet()).isEqualTo(1_000L);
    }

    @Test
    void a_short_position_is_negative_rather_than_clamped_at_zero() {
        NetPosition afterShort = store.apply(trade("t-1", Side.SELL, 700L, 1_000L));

        assertThat(afterShort.netQuantity()).isEqualTo(-700L);
    }

    @Test
    void an_out_of_order_event_time_does_not_move_last_event_timestamp_backwards() {
        store.apply(trade("t-1", Side.BUY, 100L, 5_000L));
        store.apply(trade("t-2", Side.BUY, 100L, 2_000L));

        assertThat(lastEventTimestamp())
                .as("records arrive in offset order but eventTimestamp can go backwards; GREATEST holds the max")
                .isEqualTo(Instant.ofEpochMilli(5_000L));
    }

    @Test
    void gross_buy_and_gross_sell_accumulate_independently_of_the_net() {
        store.apply(trade("t-1", Side.BUY, 400L, 1_000L));
        store.apply(trade("t-2", Side.SELL, 150L, 2_000L));

        JdbcClient jdbc = JdbcClient.create(dataSource);
        assertThat(jdbc.sql("SELECT gross_buy FROM risk_position").query(Long.class).single()).isEqualTo(400L);
        assertThat(jdbc.sql("SELECT gross_sell FROM risk_position").query(Long.class).single()).isEqualTo(150L);
    }

    @Test
    void the_version_column_increments_on_every_applied_trade() {
        store.apply(trade("t-1", Side.BUY, 400L, 1_000L));
        store.apply(trade("t-2", Side.BUY, 100L, 2_000L));

        JdbcClient jdbc = JdbcClient.create(dataSource);
        assertThat(jdbc.sql("SELECT version FROM risk_position").query(Long.class).single())
                .as("ADR-008's audit intent: the row records how many times it moved")
                .isEqualTo(2L);
    }

    private long storedNet() {
        return JdbcClient.create(dataSource)
                .sql("SELECT net_quantity FROM risk_position").query(Long.class).single();
    }

    private Instant lastEventTimestamp() {
        return JdbcClient.create(dataSource)
                .sql("SELECT last_event_timestamp FROM risk_position")
                .query(Instant.class).single();
    }

    private static EnrichedTradeEvent trade(String tradeId, Side side, long quantity, long eventMillis) {
        return EnrichedTrades.withPosition(tradeId, "trader-1", "RELIANCE", side, quantity,
                Instant.ofEpochMilli(eventMillis));
    }
}
