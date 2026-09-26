package dev.engnotes.fes.riskalert;

import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.rules.EnrichedTrades;
import dev.engnotes.fes.riskalert.window.RiskVolumeWindowStore;
import dev.engnotes.fes.riskalert.window.VolumeWindow;
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
class RiskVolumeWindowStoreIntegrationTest {

    private static final String RULE_TOPIC = "ras-vwstore-rules-" + UUID.randomUUID();

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
    private RiskVolumeWindowStore store;

    private JdbcClient jdbc;

    @BeforeEach
    void clearState() {
        jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_volume_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_volume_bucket").update();
        jdbc.sql("TRUNCATE TABLE risk_volume_ticker").update();
    }

    @Test
    void the_window_excludes_the_trade_being_evaluated() {
        store.apply(trade("t-1", "RELIANCE", 100L, Instant.parse("2026-09-12T10:00:00Z")));
        store.apply(trade("t-2", "RELIANCE", 200L, Instant.parse("2026-09-12T10:01:00Z")));
        VolumeWindow window = store.apply(
                trade("t-3", "RELIANCE", 300L, Instant.parse("2026-09-12T10:02:00Z")));

        // Two prior trades, not three: the triggering trade is subtracted after its own apply.
        assertThat(window.sampleCount()).isEqualTo(2L);
        assertThat(window.sum()).isEqualByComparingTo("300");
    }

    @Test
    void the_first_trade_in_a_window_sees_an_empty_distribution() {
        VolumeWindow window = store.apply(
                trade("t-1", "INFY", 100L, Instant.parse("2026-09-12T10:00:00Z")));

        assertThat(window.sampleCount()).isZero();
    }

    @Test
    void a_redelivery_returns_the_window_the_first_delivery_saw() {
        store.apply(trade("t-1", "TCS", 100L, Instant.parse("2026-09-12T10:00:00Z")));
        EnrichedTradeEvent second = trade("t-2", "TCS", 200L, Instant.parse("2026-09-12T10:01:00Z"));

        VolumeWindow first = store.apply(second);
        store.apply(trade("t-3", "TCS", 999L, Instant.parse("2026-09-12T10:02:00Z")));
        VolumeWindow replayed = store.apply(second);

        // t-3 landed in between and moved the live window. The redelivery must still answer from
        // the pinned totals, or a replayed trade would not reproduce its verdict (ADR-035).
        assertThat(replayed).isEqualTo(first);
    }

    @Test
    void a_trade_is_counted_once_however_many_times_it_is_delivered() {
        EnrichedTradeEvent duplicate = trade("t-1", "WIPRO", 100L, Instant.parse("2026-09-12T10:00:00Z"));
        store.apply(duplicate);
        store.apply(duplicate);

        VolumeWindow window = store.apply(
                trade("t-2", "WIPRO", 100L, Instant.parse("2026-09-12T10:01:00Z")));

        assertThat(window.sampleCount()).isEqualTo(1L);
    }

    @Test
    void the_prune_cutoff_never_moves_backwards_when_a_late_trade_arrives() {
        store.apply(trade("t-1", "HDFC", 100L, Instant.parse("2026-09-12T12:00:00Z")));
        // Three hours older than the high-water mark, so outside the one-hour horizon.
        store.apply(trade("t-2", "HDFC", 100L, Instant.parse("2026-09-12T09:00:00Z")));

        VolumeWindow window = store.apply(
                trade("t-3", "HDFC", 100L, Instant.parse("2026-09-12T12:01:00Z")));

        // t-2 applied and pinned, but its bucket is outside every subsequent window, so it
        // contributes nothing and did not resurrect a pruned bucket.
        assertThat(window.sampleCount()).isEqualTo(1L);
    }

    @Test
    void a_late_trade_outside_the_horizon_still_sees_the_priors_that_are_inside_it() {
        store.apply(trade("t-1", "BAJAJ", 100L, Instant.parse("2026-09-12T12:00:00Z")));
        // Three hours older than the high-water mark, so this trade's own bucket is pruned before
        // the fold runs. Subtracting its contribution anyway would report zero priors and silence
        // the rule for a window that genuinely holds one.
        VolumeWindow window = store.apply(
                trade("t-2", "BAJAJ", 100L, Instant.parse("2026-09-12T09:00:00Z")));

        assertThat(window.sampleCount()).isEqualTo(1L);
    }

    @Test
    void buckets_outside_the_horizon_are_deleted_rather_than_accumulated() {
        store.apply(trade("t-1", "ITC", 100L, Instant.parse("2026-09-12T10:00:00Z")));
        store.apply(trade("t-2", "ITC", 100L, Instant.parse("2026-09-12T14:00:00Z")));

        Long buckets = jdbc.sql("SELECT count(*) FROM risk_volume_bucket WHERE ticker = 'ITC'")
                .query(Long.class)
                .single();

        assertThat(buckets).isEqualTo(1L);
    }

    private static EnrichedTradeEvent trade(String tradeId, String ticker, long quantity,
                                            Instant eventTimestamp) {
        return EnrichedTrades.withPosition(tradeId, "trader-1", ticker, Side.BUY, quantity, eventTimestamp);
    }
}
