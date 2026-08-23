package dev.engnotes.fes.tradeenrichment;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
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
import static org.assertj.core.api.Assertions.assertThatCode;
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

    @Test
    @DisplayName("should close cleanly once loaded, and treat a second close as a no-op")
    void should_close_cleanly_once_loaded_and_treat_a_second_close_as_a_noop() {
        // Nothing exercised close() before this test. A loader that threw out of close(), or that
        // closed the shared consumer twice, would only be caught here.
        InstrumentCache cache = new InstrumentCache();
        InstrumentCacheLoader loader = loader(cache, Duration.ofSeconds(60));
        loader.loadInitialSnapshot();
        assertThat(loader.isLoaded()).isTrue();

        assertThatCode(loader::close).doesNotThrowAnyException();
        assertThatCode(loader::close).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should close its own consumer when setup fails before the catch-up loop is reached")
    void should_close_its_own_consumer_when_setup_fails_before_the_catch_up_loop_is_reached() {
        // partitionsOf() throws for a topic with no partitions, before the catch-up loop and before
        // any ownership handoff exists. This exercises the setup-failure close path, distinct from
        // the timeout and WakeupException paths covered above: the calling thread is still the only
        // owner the consumer has ever had, and must close it itself before the exception leaves.
        //
        // allow.auto.create.topics is forced off so this is deterministic regardless of the test
        // broker's own auto-create default: partitionsFor() must return empty, not silently create
        // the topic and hand back one partition.
        InstrumentCache cache = new InstrumentCache();
        String missingTopic = "tes-ref-missing-" + UUID.randomUUID();
        Map<String, Object> properties = new HashMap<>(consumerProperties());
        properties.put("allow.auto.create.topics", false);
        InstrumentCacheLoader loader =
                new InstrumentCacheLoader(cache, properties, missingTopic, Duration.ofSeconds(30), () -> {
                });

        assertThatThrownBy(loader::loadInitialSnapshot)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no partitions");

        assertThat(loader.isLoaded()).isFalse();
        // This assertion covers the exception type and message, not the consumer's closed state:
        // KafkaConsumer exposes no public way to observe that from outside without a test seam
        // (a spy or a wrapped factory) that the loader does not currently have. Asserting only what
        // is actually observable here rather than a check that would not really prove closure.
    }

    @Test
    @DisplayName("should fail loading and report not loaded when the onLoaded callback throws")
    void should_fail_loading_and_report_not_loaded_when_the_onLoaded_callback_throws() {
        // Task 6's callback binds a metrics gauge and starts a Kafka listener container, which can
        // throw. If loaded were set before this callback ran, a caller could observe isLoaded() ==
        // true for a service whose trade listener never actually started.
        InstrumentCache cache = new InstrumentCache();
        RuntimeException callbackFailure = new RuntimeException("listener container failed to start");
        InstrumentCacheLoader loader = new InstrumentCacheLoader(cache, consumerProperties(), TOPIC,
                Duration.ofSeconds(60), () -> {
                    throw callbackFailure;
                });

        assertThatThrownBy(loader::loadInitialSnapshot).isSameAs(callbackFailure);

        assertThat(loader.isLoaded()).isFalse();
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
