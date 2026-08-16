package dev.engnotes.fes.marketdatasimulator;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import dev.engnotes.fes.events.MarketDataTickEvent;
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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the tick publisher round-trips through a real broker and a real Schema Registry.
 *
 * <p>The unit test asserts what the publisher intends. This asserts what the cache projector will
 * actually receive: Avro resolution against a registered subject, the partition key, and the
 * headers that survive serialisation.
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
@DisplayName("MarketDataTickPublisher against a real broker and Schema Registry")
class MarketDataTickPublisherIntegrationTest {

    private static final String TOPIC = "market-data.ticks";
    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Autowired
    private MarketDataTickPublisher publisher;

    @Autowired
    private ObjectProvider<TickGenerationDriver> driverProvider;

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
            client.register(TOPIC + "-value", new AvroSchema(MarketDataTickEvent.getClassSchema()));
        } catch (RestClientException e) {
            throw new IllegalStateException("Could not register the MarketDataTickEvent subject", e);
        }
    }

    @Test
    @DisplayName("should deliver a tick that a consumer can deserialise from the registry")
    void should_deliver_a_tick_that_a_consumer_can_deserialise_from_the_registry()
            throws ExecutionException, InterruptedException {
        publisher.publish(tick("RELIANCE", "corr-int-1")).get();

        MarketDataTickEvent received = consume("corr-int-1").value();
        assertThat(received.getTicker()).isEqualTo("RELIANCE");
        assertThat(received.getBidPrice()).isEqualTo(2_499.50d);
        assertThat(received.getAskPrice()).isEqualTo(2_500.50d);
        assertThat(received.getLastTradedPrice()).isEqualTo(2_500.00d);
        assertThat(received.getVolume()).isEqualTo(12_500L);
    }

    @Test
    @DisplayName("should key the delivered record on ticker")
    void should_key_the_delivered_record_on_ticker() throws Exception {
        publisher.publish(tick("TCS", "corr-int-2")).get();

        assertThat(consume("corr-int-2").key()).isEqualTo("TCS");
    }

    @Test
    @DisplayName("should deliver trace context and correlation id as headers")
    void should_deliver_trace_context_and_correlation_id_as_headers() throws Exception {
        publisher.publish(tick("INFY", "corr-int-3")).get();

        ConsumerRecord<String, MarketDataTickEvent> received = consume("corr-int-3");
        assertThat(headerValue(received, "traceparent")).isEqualTo(TRACEPARENT);
        assertThat(headerValue(received, "correlationId")).isEqualTo("corr-int-3");
    }

    @Test
    @DisplayName("should not start tick generation unless load simulation mode is enabled")
    void should_not_start_tick_generation_unless_load_simulation_mode_is_enabled() {
        // Asserted rather than assumed. If the default ever flips to enabled, this fails with a
        // clear message instead of the other tests in this class timing out after 30 seconds while
        // hunting one correlation id in a stream of generated traffic.
        assertThat(driverProvider.getIfAvailable())
                .as("FR-01.4 generation is a mode, not boot behaviour")
                .isNull();
    }

    @Test
    @DisplayName("should preserve millisecond timestamps through the round trip")
    void should_preserve_millisecond_timestamps_through_the_round_trip() throws Exception {
        publisher.publish(tick("WIPRO", "corr-int-4")).get();

        MarketDataTickEvent received = consume("corr-int-4").value();
        assertThat(received.getEventTimestamp()).isEqualTo(Instant.parse("2026-08-16T09:15:00Z"));
        assertThat(received.getProducedAt()).isEqualTo(Instant.parse("2026-08-16T09:15:00.004Z"));
    }

    /**
     * Reads from the beginning with a fresh group and returns the record this test published.
     *
     * <p>Matching on {@code correlationId} matters: the tests share one topic, records for different
     * tickers land on different partitions, and poll order across partitions is not defined.
     * Returning "whatever arrived first" would pass while the suite is small and start failing
     * intermittently as soon as another test is added. {@code MarketDataTickEvent} carries no tick
     * id, so the correlation id is the only per-record discriminator available.
     */
    private ConsumerRecord<String, MarketDataTickEvent> consume(String correlationId) {
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

        try (KafkaConsumer<String, MarketDataTickEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, MarketDataTickEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, MarketDataTickEvent> record : records) {
                    if (correlationId.equals(record.value().getCorrelationId().toString())) {
                        return record;
                    }
                }
            }
            throw new AssertionError(
                    "Tick " + correlationId + " did not arrive on " + TOPIC + " within 30 seconds");
        }
    }

    private static String headerValue(ConsumerRecord<String, MarketDataTickEvent> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("header %s is present on the delivered record", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static MarketDataTickEvent tick(String ticker, String correlationId) {
        return MarketDataTickEvent.newBuilder()
                .setTicker(ticker)
                .setBidPrice(2_499.50d)
                .setAskPrice(2_500.50d)
                .setLastTradedPrice(2_500.00d)
                .setVolume(12_500L)
                .setEventTimestamp(Instant.parse("2026-08-16T09:15:00Z"))
                .setProducedAt(Instant.parse("2026-08-16T09:15:00.004Z"))
                .setCorrelationId(correlationId)
                .setTraceContext(Map.of("traceparent", TRACEPARENT))
                .build();
    }
}
