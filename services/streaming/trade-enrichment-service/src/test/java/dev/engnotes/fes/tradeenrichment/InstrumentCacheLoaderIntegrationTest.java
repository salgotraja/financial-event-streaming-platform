package dev.engnotes.fes.tradeenrichment;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import dev.engnotes.fes.testing.KafkaAvroStack;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The readiness condition, against a real broker.
 *
 * <p>The condition is the part worth distrusting. A gate that returns too early passes both the
 * happy path and the deletion test in {@code RawTradeConsumerIntegrationTest}, so the instrument
 * count here is deliberately larger than one poll returns.
 */
@DisplayName("InstrumentCacheLoader against a real broker")
class InstrumentCacheLoaderIntegrationTest {

    private static final String TOPIC = "tes-ref-it-" + UUID.randomUUID();
    private static final int PARTITIONS = 6;
    private static final int INSTRUMENTS = 2000;
    private static final String LAST_TICKER = "TICKER-" + (INSTRUMENTS - 1);

    @BeforeAll
    static void seedTheCompactedMaster() throws Exception {
        KafkaAvroStack.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaAvroStack.bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
        }
        new CachedSchemaRegistryClient(KafkaAvroStack.schemaRegistryUrl(), 10)
                .register(TOPIC + "-value", new AvroSchema(InstrumentReferenceEvent.getClassSchema()));

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(producerProperties())) {
            for (int i = 0; i < INSTRUMENTS; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "INE-" + i, instrument(i)));
            }
            producer.flush();
        }
    }

    @Test
    @DisplayName("should not report loaded until every partition has been read to its captured end")
    void should_not_report_loaded_until_every_partition_has_been_read_to_its_captured_end() {
        // 500 instruments across 6 partitions does not arrive in one poll. A loader that stopped
        // after the first non-empty poll would report loaded with a partial map, release the trade
        // listener, and dead-letter every trade for a ticker it had not reached yet. Asserting the
        // LAST seeded ticker, not the first, is what makes this test able to see that.
        InstrumentCache cache = new InstrumentCache();
        InstrumentCacheLoader loader = loader(cache, Duration.ofSeconds(60));

        loader.loadInitialSnapshot();

        assertThat(loader.isLoaded()).isTrue();
        assertThat(cache.size()).isEqualTo(INSTRUMENTS);
        assertThat(cache.find(LAST_TICKER)).isPresent();
    }

    @Test
    @DisplayName("should fail startup rather than release a partial map when the wait times out")
    void should_fail_startup_rather_than_release_a_partial_map_when_the_wait_times_out() {
        // Zero timeout, so the condition cannot be met. The alternative to failing here is starting
        // the trade listener with whatever was folded so far, which trades one loud failure for a
        // quiet stream of dead letters.
        InstrumentCache cache = new InstrumentCache();
        InstrumentCacheLoader loader = loader(cache, Duration.ZERO);

        assertThatThrownBy(loader::loadInitialSnapshot)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("instrument master");

        assertThat(loader.isLoaded()).isFalse();
    }

    @Test
    @DisplayName("should report loaded immediately when the master is empty")
    void should_report_loaded_immediately_when_the_master_is_empty() throws Exception {
        // Every partition's captured end offset equals its beginning offset, so the condition holds
        // on the first evaluation. A loader that waited for at least one record would hang here and
        // fail startup on a legitimately empty master.
        String emptyTopic = "tes-ref-empty-" + UUID.randomUUID();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaAvroStack.bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(emptyTopic, PARTITIONS, (short) 1))).all().get();
        }
        InstrumentCache cache = new InstrumentCache();
        InstrumentCacheLoader loader = loader(cache, emptyTopic, Duration.ofSeconds(30));

        loader.loadInitialSnapshot();

        assertThat(loader.isLoaded()).isTrue();
        assertThat(cache.size()).isZero();
    }

    private static InstrumentCacheLoader loader(InstrumentCache cache, Duration timeout) {
        return loader(cache, TOPIC, timeout);
    }

    private static InstrumentCacheLoader loader(InstrumentCache cache, String topic, Duration timeout) {
        return new InstrumentCacheLoader(cache, consumerProperties(), topic, timeout, () -> {
        });
    }

    private static Map<String, Object> consumerProperties() {
        return Map.of(
                "bootstrap.servers", KafkaAvroStack.bootstrapServers(),
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer",
                "schema.registry.url", KafkaAvroStack.schemaRegistryUrl(),
                "specific.avro.reader", true,
                "enable.auto.commit", false);
    }

    private static Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        return properties;
    }

    private static InstrumentReferenceEvent instrument(int index) {
        return InstrumentReferenceEvent.newBuilder()
                .setInstrumentId("INE-" + index)
                .setTicker("TICKER-" + index)
                .setExchange("NSE")
                .setIsin("INE-" + index)
                .setSecurityType("EQUITY")
                .setCurrency("INR")
                .setSector("ENERGY")
                .setSharesOutstanding(1_000_000L + index)
                .setReferenceVersion(1L)
                .setEffectiveAt(Instant.ofEpochMilli(1_000L))
                .setProducerIdentity("reference-data-service")
                .build();
    }
}
