package dev.engnotes.fes.positionexposure;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.sql.DataSource;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.testing.KafkaAvroStack;
import dev.engnotes.fes.testing.PostgresStack;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A snapshot publish that cannot complete, against a real broker, registry and database (ADR-027).
 * The trade is already applied by the time the send fails, so the trade is not at fault and must
 * not be dead-lettered: the container pauses and the publish is retried until it succeeds.
 *
 * <p>The failure is a real one the service can meet in production: the service runs with
 * {@code auto.register.schemas=false}, so while the output topic's value subject is not registered
 * every send fails in the Avro serializer. Registering the subject is the dependency coming back.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("PositionExposureKafkaConfiguration when the snapshot publish fails")
class SnapshotPublishOutageIntegrationTest {

    private static final String TRADE_TOPIC = "pes-pub-outage-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "pes-pub-outage-out-it-" + UUID.randomUUID();
    private static final String DLQ_TOPIC = TRADE_TOPIC + DeadLetterPublisher.DLQ_SUFFIX;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        PostgresStack.start();
        PositionExposureTestKafka.createTopic(TRADE_TOPIC, 1);
        PositionExposureTestKafka.createTopic(OUTPUT_TOPIC, 1);
        PositionExposureTestKafka.createTopic(DLQ_TOPIC, 1);
        // Deliberately no registration for OUTPUT_TOPIC: that is the failure under test. The DLQ
        // subject is registered so a misclassification would be observable rather than failing too.
        PositionExposureTestKafka.registerSchema(DLQ_TOPIC, DeadLetterEvent.getClassSchema());
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

    private static KafkaConsumer<String, DeadLetterEvent> dlqConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "assert-pub-outage-dlq-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put("specific.avro.reader", true);
        KafkaConsumer<String, DeadLetterEvent> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(DLQ_TOPIC));
        return consumer;
    }

    private static List<String> deadLettersOver(KafkaConsumer<String, DeadLetterEvent> dlq, Duration window) {
        List<String> keys = new ArrayList<>();
        Instant deadline = Instant.now().plus(window);
        while (Instant.now().isBefore(deadline)) {
            dlq.poll(Duration.ofMillis(500)).forEach(record -> keys.add(record.key()));
        }
        return keys;
    }

    @Test
    void a_failed_snapshot_publish_pauses_and_retries_rather_than_dead_lettering_an_applied_trade() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = PositionExposureTestKafka.producer();
             KafkaConsumer<String, DeadLetterEvent> dlq = dlqConsumer();
             KafkaConsumer<String, PositionSnapshotEvent> snapshots =
                     PositionExposureTestKafka.snapshotConsumer(OUTPUT_TOPIC)) {

            producer.send(new ProducerRecord<>(TRADE_TOPIC, "PUB-OUTAGE",
                    PositionExposureTestKafka.trade("T-PUB-OUTAGE-1", "acc-1", "trader-1", "PUB-OUTAGE",
                            Side.BUY, 10L, 100.0, Instant.ofEpochMilli(1_000L))));
            producer.flush();

            // A registry miss fails in milliseconds, so the poison budget (two retries, at most 5s
            // elapsed) would be spent well inside this window.
            assertThat(deadLettersOver(dlq, Duration.ofSeconds(15)))
                    .as("a failed publish is a dependency outage, not a bad trade (ADR-027)")
                    .isEmpty();

            PositionExposureTestKafka.registerSchema(OUTPUT_TOPIC, PositionSnapshotEvent.getClassSchema());

            List<ConsumerRecord<String, PositionSnapshotEvent>> published =
                    PositionExposureTestKafka.drainRecords(snapshots, 1, Duration.ofSeconds(30));
            assertThat(published.getFirst().value().getLastProcessedTradeId()).hasToString("T-PUB-OUTAGE-1");
        }

        JdbcClient jdbc = JdbcClient.create(dataSource);
        assertThat(jdbc.sql("SELECT count(*) FROM position_applied_trade WHERE trade_id = ?")
                .param("T-PUB-OUTAGE-1")
                .query(Long.class)
                .single())
                .as("the trade was applied and its ledger row committed; the primary key bounds it to one")
                .isEqualTo(1L);
        assertThat(jdbc.sql("SELECT net_quantity FROM position"
                        + " WHERE account_id = ? AND trader_id = ? AND ticker = ?")
                .params("acc-1", "trader-1", "PUB-OUTAGE")
                .query(Long.class)
                .single())
                .as("every retry re-ran the apply, and the ledger claim kept the net to one application")
                .isEqualTo(10L);
    }
}
