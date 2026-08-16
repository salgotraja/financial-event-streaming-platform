package dev.engnotes.fes.corporateactionproducer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.events.CorporateActionEvent;
import dev.engnotes.fes.events.CorporateActionType;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the publisher round-trips through a real broker and a real Schema Registry.
 *
 * <p>The enum and the open attribute map are the parts most likely to break on the wire rather than
 * in the object graph, so both are asserted after deserialisation rather than before serialisation.
 *
 * <p>Schemas are registered explicitly rather than by the producer. Production sets
 * {@code auto.register.schemas=false} so a deploy cannot quietly introduce a new schema version and
 * route around the compatibility gate (ADR-029); the test mirrors that.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("CorporateActionPublisher against a real broker and Schema Registry")
class CorporateActionPublisherIntegrationTest {

    private static final String TOPIC = "corporate-actions";

    @Autowired
    private CorporateActionPublisher publisher;

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
            client.register(TOPIC + "-value", new AvroSchema(CorporateActionEvent.getClassSchema()));
        } catch (RestClientException e) {
            throw new IllegalStateException("Could not register the CorporateActionEvent subject", e);
        }
    }

    @Test
    @DisplayName("should deliver an action that a consumer can deserialise from the registry")
    void should_deliver_an_action_that_a_consumer_can_deserialise_from_the_registry() throws Exception {
        publisher.publish(CorporateActions.split("CA-INT-1", "RELIANCE")).get();

        CorporateActionEvent received = consume("CA-INT-1").value();
        assertThat(received.getTicker()).isEqualTo("RELIANCE");
        assertThat(received.getActionType()).isEqualTo(CorporateActionType.STOCK_SPLIT);
        assertThat(received.getAnnouncedAt()).isEqualTo(CorporateActions.ANNOUNCED_AT);
        assertThat(received.getEffectiveAt()).isEqualTo(CorporateActions.EFFECTIVE_AT);
    }

    @Test
    @DisplayName("should preserve the type specific attribute map through the round trip")
    void should_preserve_the_type_specific_attribute_map_through_the_round_trip() throws Exception {
        publisher.publish(CorporateActions.rightsIssue("CA-INT-2", "INFY")).get();

        assertThat(consume("CA-INT-2").value().getAttributes())
                .containsEntry("ratio", "1:15")
                .containsEntry("subscriptionPrice", "1257.00");
    }

    @Test
    @DisplayName("should deliver an empty attribute map rather than null")
    void should_deliver_an_empty_attribute_map_rather_than_null() throws Exception {
        // The schema defaults attributes to {}. A consumer that reads an earnings announcement must
        // get an empty map to iterate, not a null to guard.
        publisher.publish(CorporateActions.earnings("CA-INT-3", "WIPRO")).get();

        assertThat(consume("CA-INT-3").value().getAttributes()).isNotNull().isEmpty();
    }

    @Test
    @DisplayName("should key the delivered record on ticker")
    void should_key_the_delivered_record_on_ticker() throws Exception {
        publisher.publish(CorporateActions.dividend("CA-INT-4", "TCS")).get();

        assertThat(consume("CA-INT-4").key()).isEqualTo("TCS");
    }

    @Test
    @DisplayName("should deliver trace context and correlation id as headers")
    void should_deliver_trace_context_and_correlation_id_as_headers() throws Exception {
        publisher.publish(CorporateActions.split("CA-INT-5", "HDFCBANK")).get();

        ConsumerRecord<String, CorporateActionEvent> received = consume("CA-INT-5");
        assertThat(headerValue(received, "traceparent")).isEqualTo(CorporateActions.TRACEPARENT);
        assertThat(headerValue(received, "correlationId")).isEqualTo("corr-1");
    }

    /**
     * Reads from the beginning with a fresh group and returns the record this test published.
     *
     * <p>Matching on {@code corporateActionId} matters: the tests share one topic, records for
     * different tickers land on different partitions, and poll order across partitions is not
     * defined. Returning "whatever arrived first" would pass while the suite is small and start
     * failing intermittently as soon as another test is added.
     */
    private ConsumerRecord<String, CorporateActionEvent> consume(String corporateActionId) {
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

        try (KafkaConsumer<String, CorporateActionEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, CorporateActionEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, CorporateActionEvent> record : records) {
                    if (corporateActionId.equals(record.value().getCorporateActionId().toString())) {
                        return record;
                    }
                }
            }
            throw new AssertionError("Corporate action " + corporateActionId
                    + " did not arrive on " + TOPIC + " within 30 seconds");
        }
    }

    private static String headerValue(ConsumerRecord<String, CorporateActionEvent> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("header %s is present on the delivered record", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
