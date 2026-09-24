package dev.engnotes.fes.positionexposure;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.testing.KafkaAvroStack;
import dev.engnotes.fes.testing.PostgresStack;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * The consumer, the store and the publisher end to end: a real broker, a real Schema Registry and a
 * real PostgreSQL. One trade produces exactly one snapshot, keyed on a hash of the composite position
 * key and carrying the consumed record's trace headers, and a redelivered trade applies once.
 *
 * <p>Every topic is unique to this class, so another module's tests cannot race these assertions.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("EnrichedTradeConsumer against a real broker, registry and database")
class PositionSnapshotIntegrationTest {

    private static final String TRADE_TOPIC = "pes-snap-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "pes-snap-out-it-" + UUID.randomUUID();

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        PostgresStack.start();
        PositionExposureTestKafka.createTopic(TRADE_TOPIC, 1);
        PositionExposureTestKafka.createTopic(OUTPUT_TOPIC, 1);
        PositionExposureTestKafka.registerSchema(OUTPUT_TOPIC, PositionSnapshotEvent.getClassSchema());
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.position-exposure-service.topic", () -> TRADE_TOPIC);
        registry.add("fes.position-exposure-service.output-topic", () -> OUTPUT_TOPIC);
        registry.add("spring.datasource.url", PostgresStack::jdbcUrl);
        registry.add("spring.datasource.username", PostgresStack::username);
        registry.add("spring.datasource.password", PostgresStack::password);
    }

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void clearPositions() {
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("TRUNCATE TABLE position_applied_trade").update();
        jdbc.sql("TRUNCATE TABLE position").update();
    }

    @Test
    void one_trade_publishes_exactly_one_snapshot_keyed_on_a_hash_of_the_position_key() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = PositionExposureTestKafka.producer();
             KafkaConsumer<String, PositionSnapshotEvent> consumer =
                     PositionExposureTestKafka.snapshotConsumer(OUTPUT_TOPIC)) {

            ProducerRecord<String, EnrichedTradeEvent> trade = new ProducerRecord<>(TRADE_TOPIC, "RELIANCE",
                    PositionExposureTestKafka.trade("t-1", "acc-1", "trader-1", "RELIANCE", Side.BUY,
                            100L, 2500.0, Instant.ofEpochMilli(1_000L)));
            trade.headers().add("traceparent", TRACEPARENT.getBytes(StandardCharsets.UTF_8));
            trade.headers().add("correlationId", "corr-t-1".getBytes(StandardCharsets.UTF_8));
            producer.send(trade);
            producer.flush();

            List<ConsumerRecord<String, PositionSnapshotEvent>> records =
                    PositionExposureTestKafka.drainRecords(consumer, 1, Duration.ofSeconds(30));

            ConsumerRecord<String, PositionSnapshotEvent> record = records.getFirst();
            assertThat(record.key())
                    .isEqualTo(IdempotencyKeys.deterministic("acc-1", "trader-1", "RELIANCE").toString());
            assertThat(new String(record.headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8))
                    .isEqualTo(TRACEPARENT);
            assertThat(new String(record.headers().lastHeader("correlationId").value(), StandardCharsets.UTF_8))
                    .isEqualTo("corr-t-1");
            assertThat(record.headers().lastHeader("tracestate")).isNull();

            PositionSnapshotEvent snapshot = record.value();
            assertThat(snapshot.getAccountId()).hasToString("acc-1");
            assertThat(snapshot.getTraderId()).hasToString("trader-1");
            assertThat(snapshot.getTicker()).hasToString("RELIANCE");
            assertThat(snapshot.getNetQuantity()).isEqualTo(100L);
            assertThat(snapshot.getGrossBuyQuantity()).isEqualTo(100L);
            assertThat(snapshot.getGrossSellQuantity()).isEqualTo(0L);
            assertThat(snapshot.getMarketValue()).isEqualTo(250_000.0);
            assertThat(snapshot.getAsOf()).isEqualTo(Instant.ofEpochMilli(1_000L));
            assertThat(snapshot.getLastProcessedTradeId()).hasToString("t-1");
        }
    }

    @Test
    void the_same_trade_delivered_twice_applies_once_and_republishes_an_identical_snapshot() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = PositionExposureTestKafka.producer();
             KafkaConsumer<String, PositionSnapshotEvent> consumer =
                     PositionExposureTestKafka.snapshotConsumer(OUTPUT_TOPIC)) {

            EnrichedTradeEvent trade = PositionExposureTestKafka.trade("t-dup", "acc-2", "trader-2", "INFY",
                    Side.BUY, 100L, 1500.0, Instant.ofEpochMilli(3_000L));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "INFY", trade));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "INFY", trade));
            producer.flush();

            // Both deliveries have been processed once two snapshots are on the topic, so the
            // database assertions below cannot pass merely because the second one has not run yet.
            List<ConsumerRecord<String, PositionSnapshotEvent>> records =
                    PositionExposureTestKafka.drainRecords(consumer, 2, Duration.ofSeconds(30));

            PositionSnapshotEvent first = records.get(0).value();
            PositionSnapshotEvent second = records.get(1).value();
            assertThat(second.getSnapshotId()).isEqualTo(first.getSnapshotId());
            assertThat(second).isEqualTo(first);
            assertThat(records.get(1).key()).isEqualTo(records.get(0).key());
            assertThat(first.getNetQuantity()).isEqualTo(100L);
            assertThat(first.getGrossBuyQuantity()).isEqualTo(100L);

            JdbcClient jdbc = JdbcClient.create(dataSource);
            assertThat(jdbc.sql("SELECT count(*) FROM position_applied_trade WHERE trade_id = 't-dup'")
                    .query(Long.class).single()).isEqualTo(1L);
            assertThat(jdbc.sql("""
                            SELECT net_quantity FROM position
                            WHERE account_id = 'acc-2' AND trader_id = 'trader-2' AND ticker = 'INFY'
                            """)
                    .query(Long.class).single()).isEqualTo(100L);
        }
    }
}
