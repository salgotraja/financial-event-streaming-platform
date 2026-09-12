package dev.engnotes.fes.riskalert;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;

import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.rules.EnrichedTrades;
import dev.engnotes.fes.testing.KafkaAvroStack;
import dev.engnotes.fes.testing.PostgresStack;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code UnusualVolumeRule} against a real broker, registry and database, proving the bootstrap
 * governance path: {@code unusual-volume} is version 0 from {@code application.yml}.
 *
 * <p>Every topic is unique to this class, so another module's tests cannot race these assertions.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("UnusualVolumeRule against a real broker, registry and database")
class UnusualVolumeIntegrationTest {

    private static final String TRADE_TOPIC = "ras-uv-it-" + UUID.randomUUID();
    private static final String RULE_TOPIC = "ras-uv-rules-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "ras-uv-out-it-" + UUID.randomUUID();
    private static final String TICKER = "UVOLIT";

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        PostgresStack.start();
        RiskAlertTestKafka.createTopic(TRADE_TOPIC, 1);
        RiskAlertTestKafka.createTopic(RULE_TOPIC, 6);
        RiskAlertTestKafka.createTopic(OUTPUT_TOPIC, 1);
        RiskAlertTestKafka.registerSchema(OUTPUT_TOPIC, RiskAlertEvent.getClassSchema());
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.risk-alert-service.topic", () -> TRADE_TOPIC);
        registry.add("fes.risk-alert-service.rule-topic", () -> RULE_TOPIC);
        registry.add("fes.risk-alert-service.output-topic", () -> OUTPUT_TOPIC);
        registry.add("spring.datasource.url", PostgresStack::jdbcUrl);
        registry.add("spring.datasource.username", PostgresStack::username);
        registry.add("spring.datasource.password", PostgresStack::password);
    }

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void clearVolumeState() {
        // Container-local schema created for this run; leaves no rows behind for the next test's
        // arithmetic, the same reason PositionLimitIntegrationTest truncates its own tables.
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_volume_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_volume_bucket").update();
        jdbc.sql("TRUNCATE TABLE risk_volume_ticker").update();
        jdbc.sql("TRUNCATE TABLE risk_position_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_position").update();
    }

    @Test
    void an_outsized_trade_after_a_uniform_window_raises_one_unusual_volume_alert() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, RiskAlertEvent> consumer =
                     RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            Instant start = Instant.parse("2026-09-12T10:00:00Z");
            // 40 uniform trades of quantity 100 build a window whose standard deviation is zero and
            // whose sample count clears the governed minimum of 30 well before the last of them.
            // None of these alerts on its own: each compares equal to, never above, the mean.
            for (int i = 0; i < 40; i++) {
                producer.send(new ProducerRecord<>(TRADE_TOPIC, TICKER,
                        EnrichedTrades.withPosition("uv-" + i, "trader-uvol", TICKER, Side.BUY, 100L,
                                start.plusSeconds(i))));
            }
            // Outsized: a zero standard deviation means any quantity above the mean of 100 crosses
            // both bands at once, so this is expected to land as CRITICAL.
            producer.send(new ProducerRecord<>(TRADE_TOPIC, TICKER,
                    EnrichedTrades.withPosition("uv-outsized", "trader-uvol", TICKER, Side.BUY, 1_000L,
                            start.plusSeconds(40))));
            producer.flush();

            var alerts = RiskAlertTestKafka.drain(consumer, 1, Duration.ofSeconds(30)).stream()
                    .filter(alert -> alert.getAlertType() == AlertType.UNUSUAL_VOLUME)
                    .toList();

            assertThat(alerts).hasSize(1);
            assertThat(alerts.getFirst().getTriggeringTradeId()).hasToString("uv-outsized");
            assertThat(alerts.getFirst().getRuleId()).hasToString("unusual-volume");
            // Version 0 is the ungoverned bootstrap set from application.yml. Nothing writes
            // risk-rules.events until Phase 5, so this is the honest value rather than a fiction.
            assertThat(alerts.getFirst().getRuleVersion()).isZero();
        }
    }
}
