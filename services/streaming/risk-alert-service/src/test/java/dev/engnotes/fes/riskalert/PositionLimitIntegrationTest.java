package dev.engnotes.fes.riskalert;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
import org.awaitility.Awaitility;
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
 * The position-limit rule end to end: a real broker, a real Schema Registry and a real PostgreSQL.
 *
 * <p>Every topic is unique to this class, so another module's tests cannot race these assertions.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("PositionLimitRule against a real broker, registry and database")
class PositionLimitIntegrationTest {

    private static final String TRADE_TOPIC = "ras-pl-it-" + UUID.randomUUID();
    private static final String RULE_TOPIC = "ras-pl-rules-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "ras-pl-out-it-" + UUID.randomUUID();

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
    void clearPositions() {
        // Truncate, not an unqualified delete: this is a container-local schema created for this
        // run, and the two statements must leave no rows behind for the next test's arithmetic.
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE risk_position_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE risk_position").update();
    }

    @Test
    void a_trader_crossing_the_bootstrap_limit_over_several_trades_raises_one_breach() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, RiskAlertEvent> consumer =
                     RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            // 6,000 then 6,000 crosses the 10,000 warning band on the second trade, not the first.
            // Both are keyed on the ticker, matching how trade-producer keys trades.raw, so they
            // land on one partition and apply in order.
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "RELIANCE",
                    EnrichedTrades.withPosition("t-1", "trader-1", "RELIANCE", Side.BUY, 6_000L,
                            Instant.ofEpochMilli(1_000L))));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "RELIANCE",
                    EnrichedTrades.withPosition("t-2", "trader-1", "RELIANCE", Side.BUY, 6_000L,
                            Instant.ofEpochMilli(2_000L))));
            producer.flush();

            List<RiskAlertEvent> breaches =
                    RiskAlertTestKafka.drain(consumer, 1, Duration.ofSeconds(30)).stream()
                            .filter(alert -> alert.getAlertType() == AlertType.POSITION_LIMIT_BREACH)
                            .toList();

            assertThat(breaches).hasSize(1);
            assertThat(breaches.getFirst().getTriggeringTradeId())
                    .as("the first trade is inside the band; only the second breaches")
                    .hasToString("t-2");
            assertThat(breaches.getFirst().getMeasuredValues())
                    .containsEntry("net-position-quantity", "12000");
            assertThat(breaches.getFirst().getRuleId()).hasToString("position-limit");
            // Version 0 is the ungoverned bootstrap set from application.yml. Nothing writes
            // risk-rules.events until Phase 5, so this is the honest value rather than a fiction.
            assertThat(breaches.getFirst().getRuleVersion()).isZero();
        }
    }

    @Test
    void both_trades_are_counted_once_in_the_store() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer()) {
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "TCS",
                    EnrichedTrades.withPosition("t-3", "trader-2", "TCS", Side.BUY, 500L,
                            Instant.ofEpochMilli(1_000L))));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "TCS",
                    EnrichedTrades.withPosition("t-4", "trader-2", "TCS", Side.SELL, 200L,
                            Instant.ofEpochMilli(2_000L))));
            producer.flush();
        }

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(JdbcClient.create(dataSource)
                        .sql("SELECT net_quantity FROM risk_position WHERE trader_id = ? AND ticker = ?")
                        .params("trader-2", "TCS")
                        .query(Long.class)
                        .optional())
                        .contains(300L));
    }
}
