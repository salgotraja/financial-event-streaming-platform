package dev.engnotes.fes.positionexposure;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves ADR-027's two failure classes end to end, against a real broker and the real container
 * registry: a poison record is quarantined and the record behind it on the same partition still
 * gets applied, and a null-valued record is quarantined on the first attempt.
 *
 * <p>Both tests share one {@code TRADE_TOPIC} and {@code DLQ_TOPIC}: {@code @DynamicPropertySource}
 * runs once for the whole class, before the one shared context starts, so a per-method topic is not
 * an option here. Each test scopes its own assertion to the record carrying its own key, following
 * {@code QuarantineIntegrationTest} in risk-alert-service.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("Poison record quarantine against a real broker and registry")
class QuarantineIntegrationTest {

    private static final String TRADE_TOPIC = "pes-poison-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "pes-poison-out-it-" + UUID.randomUUID();
    private static final String DLQ_TOPIC = TRADE_TOPIC + DeadLetterPublisher.DLQ_SUFFIX;

    // A four-byte magic-and-schema-id prefix followed by a body that is not the schema it names.
    private static final byte[] POISON = {0, 0, 0, 0, 1, 42};

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        PostgresStack.start();
        PositionExposureTestKafka.createTopic(TRADE_TOPIC, 1);
        PositionExposureTestKafka.createTopic(OUTPUT_TOPIC, 1);
        PositionExposureTestKafka.createTopic(DLQ_TOPIC, 1);
        PositionExposureTestKafka.registerSchema(OUTPUT_TOPIC, PositionSnapshotEvent.getClassSchema());
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

    private static KafkaConsumer<String, DeadLetterEvent> dlqConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "assert-dlq-" + UUID.randomUUID());
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

    private static KafkaProducer<String, byte[]> rawProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static DeadLetterEvent quarantinedRecordFor(KafkaConsumer<String, DeadLetterEvent> dlq,
                                                        String key, Duration within) {
        List<ConsumerRecord<String, DeadLetterEvent>> seen = new ArrayList<>();
        Awaitility.await().atMost(within).untilAsserted(() -> {
            ConsumerRecords<String, DeadLetterEvent> polled = dlq.poll(Duration.ofMillis(500));
            polled.forEach(seen::add);
            assertThat(seen).anyMatch(record -> key.equals(record.key()));
        });
        return seen.stream()
                .filter(record -> key.equals(record.key()))
                .findFirst()
                .orElseThrow()
                .value();
    }

    @Test
    void a_malformed_record_is_quarantined_and_the_record_behind_it_is_still_applied() {
        try (KafkaProducer<String, byte[]> raw = rawProducer();
             KafkaProducer<String, EnrichedTradeEvent> producer = PositionExposureTestKafka.producer();
             KafkaConsumer<String, DeadLetterEvent> dlq = dlqConsumer();
             KafkaConsumer<String, PositionSnapshotEvent> snapshots =
                     PositionExposureTestKafka.snapshotConsumer(OUTPUT_TOPIC)) {

            raw.send(new ProducerRecord<>(TRADE_TOPIC, "POISON", POISON));
            raw.flush();
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "POISON",
                    PositionExposureTestKafka.trade("trade-behind", "acc-1", "trader-1", "POISON",
                            Side.BUY, 10L, 100.0, Instant.ofEpochMilli(1_000L))));
            producer.flush();

            // Same key, so both records land on the same partition. That is the whole point: if
            // quarantine were per event type rather than per record, or if the offset were not
            // advanced past the poison, the second record would never be applied (ADR-027).
            DeadLetterEvent quarantined = quarantinedRecordFor(dlq, "POISON", Duration.ofSeconds(30));
            assertThat(quarantined.getOriginalTopic()).hasToString(TRADE_TOPIC);
            assertThat(quarantined.getOriginalPayload().array()).isEqualTo(POISON);

            assertThat(PositionExposureTestKafka.drain(snapshots, 1, Duration.ofSeconds(30)))
                    .singleElement()
                    .satisfies(snapshot -> assertThat(snapshot.getLastProcessedTradeId())
                            .hasToString("trade-behind"));
        }
    }

    @Test
    void a_null_valued_record_is_quarantined_on_the_first_attempt() {
        try (KafkaProducer<String, byte[]> raw = rawProducer();
             KafkaConsumer<String, DeadLetterEvent> dlq = dlqConsumer()) {

            raw.send(new ProducerRecord<>(TRADE_TOPIC, "NULLVALUE", null));
            raw.flush();

            // A genuinely null value with no DeserializationException header. retryCount pins this
            // to the same zero-retry class as the malformed-record verdict above: without the
            // guard, store.apply(null) throws NullPointerException, which is not registered
            // not-retryable, and this record is only attempted a third time before it recovers.
            DeadLetterEvent quarantined =
                    quarantinedRecordFor(dlq, "NULLVALUE", Duration.ofSeconds(30));
            assertThat(quarantined.getOriginalTopic()).hasToString(TRADE_TOPIC);
            assertThat(quarantined.getExceptionClass())
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(quarantined.getRetryCount())
                    .as("a null value is not a recoverable failure, so this must quarantine on the "
                            + "first attempt rather than spending the bounded back-off")
                    .isEqualTo(1);
        }
    }
}
