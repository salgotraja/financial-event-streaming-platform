package dev.engnotes.fes.marketdatasimulator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves load simulation mode actually reaches the broker: the driver starts with the context, the
 * generated ticks serialise against the registered subject, and every one keys on its ticker.
 *
 * <p>Writes to its own topic rather than {@code market-data.ticks}. The publisher integration test
 * scans that topic for a single correlation id, and a generator running alongside it would bury the
 * record it is looking for under continuous traffic.
 *
 * <p>This is a correctness test, not a throughput measurement. The rate here is deliberately tiny.
 * The NFR-01.1 sustained-rate result is a Phase 8 load test and nothing here stands in for it.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false",
        "fes.market-data-simulator.topic=" + TickGenerationIntegrationTest.TOPIC,
        "fes.market-data-simulator.generation.enabled=true",
        "fes.market-data-simulator.generation.rate-per-second=200",
        "fes.market-data-simulator.generation.batch-interval=10ms",
        // Also proves the instrument universe binds from configuration. The default lives in the
        // properties compact constructor, so without this the bound path for this map is never
        // exercised and a binding failure would surface in production as a silently ignored universe.
        "fes.market-data-simulator.model.instruments.HDFCBANK=1700"
})
@DisplayName("Tick generation against a real broker and Schema Registry")
class TickGenerationIntegrationTest {

    static final String TOPIC = "market-data.ticks.generation-it";

    @Autowired
    private TickGenerationDriver driver;

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
    @DisplayName("should start generating with the context and deliver well formed ticks")
    void should_start_generating_with_the_context_and_deliver_well_formed_ticks() {
        assertThat(driver.isRunning())
                .as("generation is enabled for this context, so the driver starts with it")
                .isTrue();

        List<ConsumerRecord<String, MarketDataTickEvent>> received = consume(25);

        assertThat(received).hasSizeGreaterThanOrEqualTo(25);
        received.forEach(record -> {
            MarketDataTickEvent tick = record.value();
            assertThat(record.key())
                    .as("every tick keys on its ticker so the cache projection stays ordered")
                    .isEqualTo(tick.getTicker().toString());
            assertThat(tick.getBidPrice()).isLessThanOrEqualTo(tick.getLastTradedPrice());
            assertThat(tick.getLastTradedPrice()).isLessThanOrEqualTo(tick.getAskPrice());
            assertThat(tick.getVolume()).isGreaterThanOrEqualTo(100L);
            assertThat(record.headers().lastHeader("correlationId")).isNotNull();
        });
    }

    @Test
    @DisplayName("should generate only the instrument universe bound from configuration")
    void should_generate_only_the_instrument_universe_bound_from_configuration() {
        // The default universe is four NSE tickers, set in the properties compact constructor. This
        // context configures one instrument instead, so seeing anything else means the configured
        // universe was ignored.
        assertThat(consume(25))
                .extracting(record -> record.value().getTicker().toString())
                .containsOnly("HDFCBANK");
    }

    private List<ConsumerRecord<String, MarketDataTickEvent>> consume(int atLeast) {
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

        List<ConsumerRecord<String, MarketDataTickEvent>> collected = new ArrayList<>();
        try (KafkaConsumer<String, MarketDataTickEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline && collected.size() < atLeast) {
                ConsumerRecords<String, MarketDataTickEvent> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
            }
        }
        if (collected.size() < atLeast) {
            throw new AssertionError("Only " + collected.size() + " generated ticks arrived on "
                    + TOPIC + " within 30 seconds, expected " + atLeast);
        }
        return collected;
    }
}
