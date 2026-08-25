package dev.engnotes.fes.riskalert;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.testing.KafkaAvroStack;
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
 * gets evaluated, and a trade that fails validation is quarantined rather than silently dropped.
 *
 * <p>Both tests share one {@code TRADE_TOPIC} and {@code DLQ_TOPIC}: {@code @DynamicPropertySource}
 * runs once for the whole class, before the one shared context starts, so a per-method topic is not
 * an option here, and either test's DLQ consumer may see the other test's quarantined record on the
 * same topic regardless of execution order. Each test therefore scopes its own assertion to the
 * record carrying its own key ({@code POISON} or {@code NANTICKER}, both unique to one test and one
 * record within this class) via {@link #quarantinedRecordFor}, rather than asserting over every
 * record either consumer happens to poll. That is what makes the assertions independent of test
 * order: no {@code @TestMethodOrder} or {@code @Order} pin is needed, and none is used here.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("Poison record quarantine against a real broker and registry")
class QuarantineIntegrationTest {

    private static final String TRADE_TOPIC = "ras-poison-it-" + UUID.randomUUID();
    private static final String RULE_TOPIC = "ras-poison-rules-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "ras-poison-out-it-" + UUID.randomUUID();
    private static final String DLQ_TOPIC = TRADE_TOPIC + DeadLetterPublisher.DLQ_SUFFIX;

    // A four-byte magic-and-schema-id prefix followed by a body that is not the schema it names.
    private static final byte[] POISON = {0, 0, 0, 0, 1, 42};

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        // RuleTimelineLoader assigns partitions directly rather than subscribing, so the rule
        // topic must exist before the context starts: see RiskAlertTestKafka.createTopic. The DLQ
        // topic and its subject must exist too: application.yml sets auto.register.schemas: false
        // for this service's own producer, which is what DeadLetterPublisher's KafkaTemplate uses,
        // so the first quarantine would otherwise fail with a schema-not-found SerializationException
        // rather than reach the DLQ at all, matching RawTradeConsumerIntegrationTest's DLQ setup in
        // trade-enrichment-service.
        RiskAlertTestKafka.createTopic(TRADE_TOPIC, 1);
        RiskAlertTestKafka.createTopic(RULE_TOPIC, 6);
        RiskAlertTestKafka.createTopic(OUTPUT_TOPIC, 1);
        RiskAlertTestKafka.createTopic(DLQ_TOPIC, 1);
        RiskAlertTestKafka.registerSchema(OUTPUT_TOPIC, RiskAlertEvent.getClassSchema());
        RiskAlertTestKafka.registerSchema(DLQ_TOPIC, DeadLetterEvent.getClassSchema());
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.risk-alert-service.topic", () -> TRADE_TOPIC);
        registry.add("fes.risk-alert-service.rule-topic", () -> RULE_TOPIC);
        registry.add("fes.risk-alert-service.output-topic", () -> OUTPUT_TOPIC);
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

    /**
     * Polls {@code dlq} until a record keyed exactly {@code key} appears, ignoring any other key on
     * the same shared topic, and returns that record's value. A plain {@link KafkaConsumer}'s
     * position advances on every {@code poll()} regardless of what the caller does with the result,
     * so records are accumulated across retries rather than re-read: a non-matching record polled on
     * one attempt is gone from the topic by the next.
     */
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
    void a_malformed_record_is_quarantined_and_the_record_behind_it_is_still_evaluated() {
        try (KafkaProducer<String, byte[]> raw = rawProducer();
             KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, DeadLetterEvent> dlq = dlqConsumer();
             KafkaConsumer<String, RiskAlertEvent> alerts = RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            raw.send(new ProducerRecord<>(TRADE_TOPIC, "POISON", POISON));
            raw.flush();
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "POISON",
                    RiskAlertTestKafka.trade("trade-behind", "POISON", 6.0,
                            Instant.ofEpochMilli(1_000L))));
            producer.flush();

            // Same key, so both records land on the same partition. That is the whole point: if
            // quarantine were per event type rather than per record, or if the offset were not
            // advanced past the poison, the second record would never be evaluated (ADR-027).
            // Scoped to the POISON key: DLQ_TOPIC is shared with the NaN test below, and a record
            // quarantined there under a different key must not satisfy this assertion.
            DeadLetterEvent quarantined = quarantinedRecordFor(dlq, "POISON", Duration.ofSeconds(30));
            assertThat(quarantined.getOriginalTopic()).hasToString(TRADE_TOPIC);
            // The delivered bytes, taken off the DeserializationException. An empty array here
            // means the recoverer passed the nulled record value through instead.
            assertThat(quarantined.getOriginalPayload().array()).isEqualTo(POISON);

            assertThat(RiskAlertTestKafka.drain(alerts, 1, Duration.ofSeconds(30)))
                    .singleElement()
                    .satisfies(alert -> assertThat(alert.getTriggeringTradeId())
                            .hasToString("trade-behind"));
        }
    }

    @Test
    void a_trade_whose_price_deviation_is_not_finite_is_quarantined_rather_than_passed_over() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, DeadLetterEvent> dlq = dlqConsumer()) {

            producer.send(new ProducerRecord<>(TRADE_TOPIC, "NANTICKER",
                    RiskAlertTestKafka.trade("trade-nan", "NANTICKER", Double.NaN,
                            Instant.ofEpochMilli(1_000L))));
            producer.flush();

            // NaN fails every comparison, so without the explicit finite check this record would
            // breach no band, produce no alert, be acknowledged, and look exactly like a clean
            // trade. Quarantining makes the bad record visible instead of losing it silently.
            // Scoped to the NANTICKER key for the same reason as the malformed-record test above.
            DeadLetterEvent quarantined =
                    quarantinedRecordFor(dlq, "NANTICKER", Duration.ofSeconds(30));
            assertThat(quarantined.getOriginalTopic()).hasToString(TRADE_TOPIC);
        }
    }
}
