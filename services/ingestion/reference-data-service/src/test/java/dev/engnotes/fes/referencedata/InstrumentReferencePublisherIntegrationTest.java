package dev.engnotes.fes.referencedata;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import io.micrometer.tracing.ScopedSpan;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the publisher round-trips through a real broker and a real Schema Registry.
 *
 * <p>Schemas are registered explicitly rather than by the producer. Production sets
 * {@code auto.register.schemas=false} so a deploy cannot quietly introduce a new schema version and
 * route around the compatibility gate (ADR-029); the test mirrors that.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false",
        // Sample everything: the trace header assertion below needs a recording span, and the
        // default probability sampler would make it intermittent.
        "management.tracing.sampling.probability=1.0"
})
@DisplayName("InstrumentReferencePublisher against a real broker and Schema Registry")
class InstrumentReferencePublisherIntegrationTest {

    private static final String TOPIC = "reference-data.instruments";

    @Autowired
    private InstrumentReferencePublisher publisher;

    @Autowired
    private Tracer tracer;

    @Autowired
    private ObjectProvider<InstrumentMasterSeeder> seederProvider;

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
    @DisplayName("should deliver an instrument that a consumer can deserialise from the registry")
    void should_deliver_an_instrument_that_a_consumer_can_deserialise_from_the_registry() throws Exception {
        publisher.publish(Instruments.instrument("INS-INT-1", "RELIANCE", 1L)).get();

        InstrumentReferenceEvent received = consume("INS-INT-1").value();
        assertThat(received.getTicker()).isEqualTo("RELIANCE");
        assertThat(received.getExchange()).isEqualTo("NSE");
        assertThat(received.getCurrency()).isEqualTo("INR");
        assertThat(received.getSharesOutstanding()).isEqualTo(6_766_000_000L);
        assertThat(received.getEffectiveAt()).isEqualTo(Instruments.EFFECTIVE_AT);
    }

    @Test
    @DisplayName("should key the delivered record on instrument id")
    void should_key_the_delivered_record_on_instrument_id() throws Exception {
        publisher.publish(Instruments.instrument("INS-INT-2", "TCS", 1L)).get();

        assertThat(consume("INS-INT-2").key()).isEqualTo("INS-INT-2");
    }

    @Test
    @DisplayName("should stamp the configured producer identity on the delivered record")
    void should_stamp_the_configured_producer_identity_on_the_delivered_record() throws Exception {
        publisher.publish(Instruments.instrument("INS-INT-3", "INFY", 1L, "impersonated")).get();

        assertThat(consume("INS-INT-3").value().getProducerIdentity())
                .isEqualTo("reference-data-service");
    }

    @Test
    @DisplayName("should deliver W3C trace context from the active span rather than the payload")
    void should_deliver_w3c_trace_context_from_the_active_span_rather_than_the_payload() throws Exception {
        // This schema carries no traceContext field, so NFR-04.1 is met by injecting the current
        // span through the Propagator. Asserting the delivered traceparent carries this span's trace
        // id is what distinguishes real propagation from a header that merely exists.
        ScopedSpan span = tracer.startScopedSpan("publish-instrument");
        String traceId = span.context().traceId();
        try {
            publisher.publish(Instruments.instrument("INS-INT-4", "WIPRO", 1L)).get();
        } finally {
            span.end();
        }

        assertThat(headerValue(consume("INS-INT-4"), "traceparent")).contains(traceId);
    }

    @Test
    @DisplayName("should not seed the instrument master unless seeding is enabled")
    void should_not_seed_the_instrument_master_unless_seeding_is_enabled() {
        // Asserted rather than assumed. If the default flips, this fails with a clear message
        // instead of the other tests in this class colliding with seeded versions of the same ids.
        assertThat(seederProvider.getIfAvailable())
                .as("a restart must not re-announce the whole master")
                .isNull();
    }

    /**
     * Reads from the beginning with a fresh group and returns the record this test published.
     *
     * <p>Matching on {@code instrumentId} matters: the tests share one topic, records for different
     * instruments land on different partitions, and poll order across partitions is not defined.
     * Returning "whatever arrived first" would pass while the suite is small and start failing
     * intermittently as soon as another test is added.
     */
    private ConsumerRecord<String, InstrumentReferenceEvent> consume(String instrumentId) {
        try (KafkaConsumer<String, InstrumentReferenceEvent> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, InstrumentReferenceEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, InstrumentReferenceEvent> record : records) {
                    if (instrumentId.equals(record.value().getInstrumentId().toString())) {
                        return record;
                    }
                }
            }
            throw new AssertionError("Instrument " + instrumentId
                    + " did not arrive on " + TOPIC + " within 30 seconds");
        }
    }

    private static Properties consumerProps() {
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
        return props;
    }

    private static String headerValue(ConsumerRecord<String, InstrumentReferenceEvent> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("header %s is present on the delivered record", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
