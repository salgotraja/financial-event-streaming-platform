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
 * {@code SelfCrossRule} against a real broker, registry and database, proving the bootstrap
 * governance path: {@code self-cross} is version 0 from {@code application.yml}.
 *
 * <p>Every topic is unique to this class, so another module's tests cannot race these assertions.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("SelfCrossRule against a real broker, registry and database")
class SelfCrossIntegrationTest {

    private static final String TRADE_TOPIC = "ras-sc-it-" + UUID.randomUUID();
    private static final String RULE_TOPIC = "ras-sc-rules-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "ras-sc-out-it-" + UUID.randomUUID();
    private static final String TICKER = "XCROSSIT";

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
    void clearRecentTradeState() {
        // Container-local schema created for this run; leaves no rows behind for the next test's
        // matching, the same reason PositionLimitIntegrationTest truncates its own tables.
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_recent_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_position_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_position").update();
    }

    @Test
    void an_offsetting_pair_raises_one_wash_trade_alert_naming_the_first_trade() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, RiskAlertEvent> consumer =
                     RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            Instant first = Instant.parse("2026-09-12T10:00:00Z");
            Instant second = first.plusSeconds(30);

            // Same trader, same ticker, same quantity and price, opposite sides, well inside the
            // bootstrap 300-second window: a self-cross by definition.
            producer.send(new ProducerRecord<>(TRADE_TOPIC, TICKER,
                    EnrichedTrades.withPosition("sc-1", "trader-cross", TICKER, Side.BUY, 500L, first)));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, TICKER,
                    EnrichedTrades.withPosition("sc-2", "trader-cross", TICKER, Side.SELL, 500L, second)));
            producer.flush();

            var alerts = RiskAlertTestKafka.drain(consumer, 1, Duration.ofSeconds(30)).stream()
                    .filter(alert -> alert.getAlertType() == AlertType.WASH_TRADE_DETECTED)
                    .toList();

            assertThat(alerts).hasSize(1);
            // The rule names the triggering trade, the one that closes the round trip, not the one
            // that opened it: sc-1 is the prior trade this alert reports as matched.
            assertThat(alerts.getFirst().getTriggeringTradeId()).hasToString("sc-2");
            assertThat(alerts.getFirst().getMeasuredValues()).containsEntry("matched-trade-id", "sc-1");
            assertThat(alerts.getFirst().getRuleId()).hasToString("self-cross");
            // Version 0 is the ungoverned bootstrap set from application.yml. Nothing writes
            // risk-rules.events until Phase 5, so this is the honest value rather than a fiction.
            assertThat(alerts.getFirst().getRuleVersion()).isZero();
        }
    }
}
