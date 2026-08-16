package dev.engnotes.fes.audit;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.testing.KafkaAvroStack;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the archival consumer against a real broker and a real Schema Registry: a healthy record
 * reaches the sink, a poison record is quarantined per record, and the partition keeps moving.
 *
 * <p>The topic is unique to this class. The service's real topic list is shared with the producer
 * modules' tests, and archiving their traffic here would make every assertion race against records
 * this test never published.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("AuditRecordConsumer against a real broker and Schema Registry")
class AuditRecordConsumerIntegrationTest {

    private static final String TOPIC = "audit-it-" + UUID.randomUUID();
    private static final String DLQ_TOPIC = TOPIC + ".dlq";

    @Autowired
    private RecordingAuditSink sink;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.audit-service.topics", () -> TOPIC);
    }

    @BeforeAll
    static void prepareBroker() throws Exception {
        KafkaAvroStack.start();
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(TOPIC, 1, (short) 1),
                    new NewTopic(DLQ_TOPIC, 1, (short) 1))).all().get();
        }
        // auto.register.schemas=false in production, so the subjects are registered here rather than
        // by the first send. The DLQ subject matters as much as the source one: an unregistered
        // dead-letter subject turns a quarantine into a second failure.
        try (CachedSchemaRegistryClient client =
                     new CachedSchemaRegistryClient(KafkaAvroStack.schemaRegistryUrl(), 10)) {
            client.register(TOPIC + "-value", new AvroSchema(TradeEvent.getClassSchema()));
            client.register(DLQ_TOPIC + "-value", new AvroSchema(DeadLetterEvent.getClassSchema()));
        }
    }

    @Test
    @DisplayName("should archive a healthy record with its event type and delivered bytes")
    void should_archive_a_healthy_record_with_its_event_type_and_delivered_bytes() {
        byte[] payload = avro(Trades.trade("T-HEALTHY"));

        publish("RELIANCE", payload);

        // Matched on the payload, which is unique to this trade id. Matching on "the first record
        // archived" would pass today and start failing the moment another test publishes here.
        ArchivedRecord archived = sink.awaitPayload(payload);
        assertThat(archived.topic()).isEqualTo(TOPIC);
        assertThat(archived.key()).isEqualTo("RELIANCE");
        assertThat(archived.eventType()).isEqualTo("TradeEvent");
    }

    @Test
    @DisplayName("should quarantine a poison record and keep archiving the records behind it")
    void should_quarantine_a_poison_record_and_keep_archiving_the_records_behind_it() {
        byte[] poison = "not an avro payload".getBytes();
        byte[] healthy = avro(Trades.trade("T-AFTER-POISON"));

        publish("INFY", poison);
        publish("INFY", healthy);

        DeadLetterEvent quarantined = consumeDeadLetter(poison);
        assertThat(quarantined.getOriginalTopic()).isEqualTo(TOPIC);
        assertThat(quarantined.getFailureReason())
                .as("the chain names the layer that classified the record as poison")
                .contains(AuditDecodeException.class.getName());
        assertThat(quarantined.getOriginalPayload().array())
                .as("the quarantined evidence is the payload that failed, not a re-encoding")
                .isEqualTo(poison);
        assertThat(quarantined.getConsumerGroup()).isEqualTo("audit-service");

        // The offset advanced past the poison record: the record published behind it is archived.
        assertThat(sink.awaitPayload(healthy)).isNotNull();
    }

    @Test
    @DisplayName("should retry a failing sink three times before quarantining the record")
    void should_retry_a_failing_sink_three_times_before_quarantining_the_record() {
        // A sink failure is transient, unlike a decode failure, so it goes through the backoff. This
        // is the only path that proves the retry listener is wired: the decode path skips retries by
        // design, so a broken listener registration would still look correct there.
        byte[] rejected = avro(Trades.trade("T-SINK-FAILS"));
        sink.rejectPayload(rejected);

        publish("TCS", rejected);

        DeadLetterEvent quarantined = consumeDeadLetter(rejected);
        assertThat(quarantined.getRetryCount())
                .as("three attempts total, so two retries after the first failure")
                .isEqualTo(3);
        assertThat(quarantined.getExceptionClass()).isEqualTo(IllegalStateException.class.getName());
        assertThat(quarantined.getFirstFailureAt())
                .as("a fallback timestamp would equal lastFailureAt; the backoff guarantees it does not")
                .isBefore(quarantined.getLastFailureAt());
    }

    @Test
    @DisplayName("should run the listener container with manual immediate acknowledgement")
    void should_run_the_listener_container_with_manual_immediate_acknowledgement() {
        // platform-common forces this on every container. Asserting it on the running container is
        // what distinguishes an applied customizer from a bean that exists.
        assertThat(listenerRegistry.getListenerContainers())
                .isNotEmpty()
                .allSatisfy(container -> assertThat(ackMode(container)).isEqualTo(AckMode.MANUAL_IMMEDIATE));
    }

    private static AckMode ackMode(MessageListenerContainer container) {
        return container.getContainerProperties().getAckMode();
    }

    private static byte[] avro(TradeEvent trade) {
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    KafkaAvroStack.schemaRegistryUrl(),
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false), false);
            return serializer.serialize(TOPIC, trade);
        }
    }

    private static void publish(String key, byte[] payload) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC, key, payload));
        }
    }

    private static DeadLetterEvent consumeDeadLetter(byte[] originalPayload) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        try (KafkaConsumer<String, DeadLetterEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(DLQ_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, DeadLetterEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, DeadLetterEvent> record : records) {
                    if (java.util.Arrays.equals(originalPayload, record.value().getOriginalPayload().array())) {
                        return record.value();
                    }
                }
            }
            throw new AssertionError("No dead letter arrived on " + DLQ_TOPIC + " within 30 seconds");
        }
    }

    @TestConfiguration
    static class RecordingSinkConfiguration {

        @Bean
        @Primary
        RecordingAuditSink recordingAuditSink() {
            return new RecordingAuditSink();
        }
    }

    static class RecordingAuditSink implements AuditSink {

        private final List<ArchivedRecord> written = new CopyOnWriteArrayList<>();
        private volatile byte[] rejected;

        void rejectPayload(byte[] payload) {
            this.rejected = payload;
        }

        @Override
        public void write(ArchivedRecord record) {
            if (java.util.Arrays.equals(rejected, record.payload())) {
                throw new IllegalStateException("sink unavailable");
            }
            written.add(record);
        }

        ArchivedRecord awaitPayload(byte[] payload) {
            return await(record -> java.util.Arrays.equals(payload, record.payload()),
                    "a record carrying the expected payload");
        }

        private ArchivedRecord await(java.util.function.Predicate<ArchivedRecord> match, String what) {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ArchivedRecord record : written) {
                    if (match.test(record)) {
                        return record;
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted waiting for " + what, e);
                }
            }
            throw new AssertionError("The archive never received " + what + " within 30 seconds");
        }
    }
}
