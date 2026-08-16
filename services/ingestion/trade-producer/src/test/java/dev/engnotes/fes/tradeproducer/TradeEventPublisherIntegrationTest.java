package dev.engnotes.fes.tradeproducer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
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
 * Proves the producer round-trips through a real broker and a real Schema Registry.
 *
 * <p>The unit test asserts what the publisher intends. This asserts what a consumer actually
 * receives: Avro resolution against a registered subject, the partition key, and the headers that
 * survive serialisation.
 *
 * <p>Schemas are registered explicitly rather than by the producer. Production sets
 * {@code auto.register.schemas=false} so a deploy cannot quietly introduce a new schema version and
 * route around the compatibility gate (ADR-029); the test mirrors that.
 */
@SpringBootTest(properties = {
        // No collector runs during tests. Left enabled, every context shutdown logs a connection
        // failure stack trace that buries real output. Set here rather than in a test
        // application.yml, which would shadow the main one instead of merging with it.
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("TradeEventPublisher against a real broker and Schema Registry")
class TradeEventPublisherIntegrationTest {

    private static final String TOPIC = "trades.raw";
    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Autowired
    private TradeEventPublisher publisher;

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
            client.register(TOPIC + "-value", new AvroSchema(TradeEvent.getClassSchema()));
        } catch (RestClientException e) {
            throw new IllegalStateException("Could not register the TradeEvent subject", e);
        }
    }

    @Test
    @DisplayName("should deliver a trade that a consumer can deserialise from the registry")
    void should_deliver_a_trade_that_a_consumer_can_deserialise_from_the_registry()
            throws ExecutionException, InterruptedException {
        TradeEvent published = trade("TRD-INT-1", "RELIANCE");

        publisher.publish(published).get();

        ConsumerRecord<String, TradeEvent> received = consume("TRD-INT-1");
        assertThat(received.value().getTradeId()).isEqualTo("TRD-INT-1");
        assertThat(received.value().getTicker()).isEqualTo("RELIANCE");
        assertThat(received.value().getPrice()).isEqualTo(2_500.00d);
        assertThat(received.value().getSide()).isEqualTo(Side.BUY);
    }

    @Test
    @DisplayName("should key the delivered record on ticker")
    void should_key_the_delivered_record_on_ticker() throws Exception {
        publisher.publish(trade("TRD-INT-2", "TCS")).get();

        assertThat(consume("TRD-INT-2").key()).isEqualTo("TCS");
    }

    @Test
    @DisplayName("should deliver trace context and correlation id as headers")
    void should_deliver_trace_context_and_correlation_id_as_headers() throws Exception {
        publisher.publish(trade("TRD-INT-3", "INFY")).get();

        ConsumerRecord<String, TradeEvent> received = consume("TRD-INT-3");
        assertThat(headerValue(received, "traceparent")).isEqualTo(TRACEPARENT);
        assertThat(headerValue(received, "correlationId")).isEqualTo("corr-1");
    }

    @Test
    @DisplayName("should leave CDC provenance null for a natively produced trade")
    void should_leave_cdc_provenance_null_for_a_natively_produced_trade() throws Exception {
        publisher.publish(trade("TRD-INT-4", "WIPRO")).get();

        TradeEvent received = consume("TRD-INT-4").value();
        assertThat(received.getSourceSystem()).isNull();
        assertThat(received.getMigrationBatchId()).isNull();
    }

    /**
     * Reads from the beginning with a fresh group and returns the record this test published.
     *
     * <p>Matching on {@code tradeId} matters: the tests share one topic, records for different
     * tickers land on different partitions, and poll order across partitions is not defined.
     * Returning "whatever arrived first" would pass while the suite is small and start failing
     * intermittently as soon as another test is added.
     */
    private ConsumerRecord<String, TradeEvent> consume(String tradeId) {
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

        try (KafkaConsumer<String, TradeEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, TradeEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, TradeEvent> record : records) {
                    if (tradeId.equals(record.value().getTradeId())) {
                        return record;
                    }
                }
            }
            throw new AssertionError(
                    "Trade " + tradeId + " did not arrive on " + TOPIC + " within 30 seconds");
        }
    }

    private static String headerValue(ConsumerRecord<String, TradeEvent> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("header %s is present on the delivered record", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static TradeEvent trade(String tradeId, String ticker) {
        return TradeEvent.newBuilder()
                .setTradeId(tradeId)
                .setCorrelationId("corr-1")
                .setTicker(ticker)
                .setQuantity(1_000L)
                .setPrice(2_500.00d)
                .setSide(Side.BUY)
                .setTraderId("TRADER-1")
                .setAccountId("ACC-1")
                .setEventTimestamp(Instant.parse("2026-08-16T09:15:00Z"))
                .setProducedAt(Instant.parse("2026-08-16T09:15:00.004Z"))
                .setTraceContext(Map.of("traceparent", TRACEPARENT))
                .build();
    }
}
