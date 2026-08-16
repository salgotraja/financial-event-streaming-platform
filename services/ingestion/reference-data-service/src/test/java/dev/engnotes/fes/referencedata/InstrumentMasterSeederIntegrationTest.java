package dev.engnotes.fes.referencedata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import dev.engnotes.fes.testing.KafkaAvroStack;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the startup seed publishes the configured instrument master (FR-10.1).
 *
 * <p>Writes to its own topic rather than {@code reference-data.instruments}. The publisher
 * integration test scans that topic for specific instrument ids, and a seed running alongside it
 * would put a second version of the same ids in play.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false",
        "fes.reference-data-service.topic=" + InstrumentMasterSeederIntegrationTest.TOPIC,
        "fes.reference-data-service.seed.enabled=true"
})
@DisplayName("Instrument master seeding against a real broker and Schema Registry")
class InstrumentMasterSeederIntegrationTest {

    static final String TOPIC = "reference-data.instruments.seed-it";

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
    }

    @BeforeAll
    static void registerSchema() throws Exception {
        KafkaAvroStack.start();
        try (CachedSchemaRegistryClient client =
                     new CachedSchemaRegistryClient(KafkaAvroStack.schemaRegistryUrl(), 10)) {
            client.register(TOPIC + "-value", new AvroSchema(InstrumentReferenceEvent.getClassSchema()));
        } catch (RestClientException e) {
            throw new IllegalStateException("Could not register the InstrumentReferenceEvent subject", e);
        }
    }

    @Test
    @DisplayName("should publish the default instrument master once at startup")
    void should_publish_the_default_instrument_master_once_at_startup() {
        List<ConsumerRecord<String, InstrumentReferenceEvent>> seeded = consumeAll(4);

        assertThat(seeded)
                .extracting(record -> record.value().getInstrumentId().toString())
                .containsExactlyInAnyOrder("INS-RELIANCE", "INS-TCS", "INS-INFY", "INS-WIPRO");
        assertThat(seeded)
                .allSatisfy(record -> assertThat(record.key())
                        .as("key is the instrument id, matching the compacted topic contract")
                        .isEqualTo(record.value().getInstrumentId().toString()));
    }

    @Test
    @DisplayName("should seed every instrument at the initial reference version")
    void should_seed_every_instrument_at_the_initial_reference_version() {
        assertThat(consumeAll(4))
                .allSatisfy(record -> {
                    assertThat(record.value().getReferenceVersion())
                            .isEqualTo(InstrumentMasterSeeder.INITIAL_VERSION);
                    assertThat(record.value().getProducerIdentity()).isEqualTo("reference-data-service");
                });
    }

    private List<ConsumerRecord<String, InstrumentReferenceEvent>> consumeAll(int atLeast) {
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

        List<ConsumerRecord<String, InstrumentReferenceEvent>> collected = new ArrayList<>();
        try (KafkaConsumer<String, InstrumentReferenceEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline && collected.size() < atLeast) {
                ConsumerRecords<String, InstrumentReferenceEvent> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
            }
        }
        if (collected.size() < atLeast) {
            throw new AssertionError("Only " + collected.size() + " seeded instruments arrived on "
                    + TOPIC + " within 30 seconds, expected " + atLeast);
        }
        return collected;
    }
}
