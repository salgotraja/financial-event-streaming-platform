package dev.engnotes.fes.riskalert.governance;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import dev.engnotes.fes.events.RiskRuleLifecycleEvent;
import dev.engnotes.fes.events.RuleState;
import dev.engnotes.fes.riskalert.rules.InvalidRuleParametersException;
import dev.engnotes.fes.riskalert.rules.PriceDeviationParameters;
import dev.engnotes.fes.testing.KafkaAvroStack;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * The fold against a real broker and a real Schema Registry.
 *
 * <p>Every topic is unique to its test, because the loader assigns every partition from the
 * beginning and a topic shared with another test would fold that test's records too.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RuleTimelineLoaderIntegrationTest {

    private static final Map<String, String> BANDS =
            Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0");

    private static final BootstrapRuleProperties NO_BOOTSTRAP =
            new BootstrapRuleProperties(List.of());

    @BeforeAll
    static void startTheStack() {
        KafkaAvroStack.start();
    }

    private static String freshTopic() {
        String topic = "ras-rules-it-" + UUID.randomUUID();
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        try (Admin admin = Admin.create(adminProperties)) {
            admin.createTopics(List.of(new NewTopic(topic, 6, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not create " + topic, e);
        }
        return topic;
    }

    private static void publish(String topic, RiskRuleLifecycleEvent... events) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());

        try (KafkaProducer<String, RiskRuleLifecycleEvent> producer = new KafkaProducer<>(properties)) {
            for (RiskRuleLifecycleEvent event : events) {
                // Keyed by ruleId, so every version of one rule lands in one partition and arrives
                // in per-rule total order. The fold relies on nothing stronger than that.
                producer.send(new ProducerRecord<>(topic, event.getRuleId().toString(), event));
            }
            producer.flush();
        }
    }

    private static void publishRawBytes(String topic, String key, byte[] value) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        // Bytes with no Confluent magic byte, so KafkaAvroDeserializer's delegate throws a
        // SerializationException while decoding this record: a genuinely undecodable payload,
        // not merely a payload whose decoded parameters are semantically wrong.
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, key, value));
            producer.flush();
        }
    }

    private static RiskRuleLifecycleEvent lifecycle(String ruleId, long version, RuleState state,
                                                    Map<String, String> parameters, long effectiveAt) {
        return RiskRuleLifecycleEvent.newBuilder()
                .setRuleId(ruleId)
                .setVersion(version)
                .setState(state)
                .setRuleType("price-deviation")
                .setParameters(Map.copyOf(parameters))
                .setMakerSubject("maker@fes.local")
                .setCheckerSubject("checker@fes.local")
                .setReason("integration test")
                .setEffectiveAt(Instant.ofEpochMilli(effectiveAt))
                .setEventTimestamp(Instant.ofEpochMilli(effectiveAt))
                .setCorrelationId(UUID.randomUUID().toString())
                .build();
    }

    private static Map<String, Object> consumerProperties() {
        Map<String, Object> properties = new java.util.HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        // ErrorHandlingDeserializer wrapping KafkaAvroDeserializer, matching
        // RiskAlertKafkaConfiguration's production wiring exactly: a plain KafkaAvroDeserializer
        // throws SerializationException out of poll() itself for an undecodable payload, before
        // RuleTimelineLoader.fold() ever runs, which is the fix-round-1 CRITICAL finding this test
        // class now covers.
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.class);
        properties.put(
                org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaAvroStack.schemaRegistryUrl());
        properties.put("specific.avro.reader", true);
        return properties;
    }

    private static RuleTimelineLoader loader(String topic, RiskRuleRegistry registry,
                                             Duration timeout, List<String> rejections) {
        return new RuleTimelineLoader(
                registry,
                consumerProperties(),
                topic,
                timeout,
                transition -> {
                    if ("price-deviation".equals(transition.ruleType())) {
                        PriceDeviationParameters.from(transition.parameters());
                    }
                },
                rejections::add,
                () -> { });
    }

    @Test
    void the_full_history_of_a_rule_is_folded_not_just_its_latest_record() {
        String topic = freshTopic();
        publish(topic,
                lifecycle("pd", 1, RuleState.ACTIVE, BANDS, 1_000L),
                lifecycle("pd", 2, RuleState.ACTIVE,
                        Map.of("warn-deviation-percent", "1.0", "critical-deviation-percent", "3.0"),
                        5_000L));

        RiskRuleRegistry registry = new RiskRuleRegistry(NO_BOOTSTRAP);
        try (RuleTimelineLoader loader = loader(topic, registry, Duration.ofSeconds(30),
                new CopyOnWriteArrayList<>())) {
            loader.loadInitialSnapshot();

            // The assertion a compacted-topic assumption would fail. On a compacted topic only the
            // latest record per key survives, so v1 would be gone and a trade that executed at
            // 2000 would resolve nothing. risk-rules.events is retention based and rule history is
            // immutable, so both versions must be present in the timeline.
            assertThat(registry.inForceAt("price-deviation", 2_000L)).singleElement()
                    .satisfies(rule -> {
                        assertThat(rule.version()).isEqualTo(1L);
                        assertThat(rule.parameters()).containsEntry("warn-deviation-percent", "2.0");
                    });
            assertThat(registry.inForceAt("price-deviation", 6_000L)).singleElement()
                    .satisfies(rule -> {
                        assertThat(rule.version()).isEqualTo(2L);
                        assertThat(rule.parameters()).containsEntry("warn-deviation-percent", "1.0");
                    });
        }
    }

    @Test
    void the_gate_opens_on_an_empty_rule_topic() {
        String topic = freshTopic();
        RiskRuleRegistry registry = new RiskRuleRegistry(NO_BOOTSTRAP);

        try (RuleTimelineLoader loader = loader(topic, registry, Duration.ofSeconds(30),
                new CopyOnWriteArrayList<>())) {
            // Nothing writes risk-rules.events until Phase 5, so an empty topic is the normal case
            // and must not hang startup. A captured end offset equal to the beginning is already
            // satisfied, which is what makes this terminate.
            loader.loadInitialSnapshot();

            assertThat(loader.isLoaded()).isTrue();
            assertThat(registry.timelineCount()).isZero();
        }
    }

    @Test
    void a_record_published_after_the_captured_end_offset_reaches_a_running_loader() {
        String topic = freshTopic();
        publish(topic, lifecycle("pd", 1, RuleState.ACTIVE, BANDS, 1_000L));

        RiskRuleRegistry registry = new RiskRuleRegistry(NO_BOOTSTRAP);
        try (RuleTimelineLoader loader = loader(topic, registry, Duration.ofSeconds(30),
                new CopyOnWriteArrayList<>())) {
            loader.loadInitialSnapshot();
            assertThat(registry.inForceAt("price-deviation", 2_000L)).singleElement()
                    .satisfies(rule -> assertThat(rule.version()).isEqualTo(1L));

            // FR-04.4: a rule change reaches a running service without a restart. This is the
            // follower thread, and it is the only thing that makes that requirement true.
            publish(topic, lifecycle("pd", 2, RuleState.ACTIVE, BANDS, 3_000L));

            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(registry.inForceAt("price-deviation", 4_000L)).singleElement()
                            .satisfies(rule -> assertThat(rule.version()).isEqualTo(2L)));
        }
    }

    @Test
    void a_governed_version_with_invalid_parameters_is_skipped_and_the_gate_still_opens() {
        String topic = freshTopic();
        publish(topic,
                // critical-deviation-percent absent, so PriceDeviationParameters rejects it.
                lifecycle("pd-broken", 1, RuleState.ACTIVE,
                        Map.of("warn-deviation-percent", "2.0"), 1_000L),
                lifecycle("pd-good", 1, RuleState.ACTIVE, BANDS, 1_000L));

        RiskRuleRegistry registry = new RiskRuleRegistry(NO_BOOTSTRAP);
        List<String> rejections = new CopyOnWriteArrayList<>();

        try (RuleTimelineLoader loader = loader(topic, registry, Duration.ofSeconds(30), rejections)) {
            // The availability assertion, and the reason this test exists. The fold gates the trade
            // listener, so throwing here would turn one bad control-plane record into a service that
            // will not start. A governance data error must never stop the streaming plane.
            loader.loadInitialSnapshot();

            assertThat(loader.isLoaded()).isTrue();
            assertThat(registry.inForceAt("price-deviation", 2_000L)).singleElement()
                    .satisfies(rule -> assertThat(rule.ruleId()).isEqualTo("pd-good"));
            assertThat(rejections).containsExactly("missing_parameter");
        }
    }

    @Test
    void an_undecodable_record_is_skipped_and_the_gate_still_opens() {
        String topic = freshTopic();
        // No Confluent magic byte, so this never reaches RuleTransition.of(...) at all: it fails
        // inside the deserializer, before fold() ever sees a decoded event. This is a different
        // and earlier failure than a decoded record with semantically invalid parameters.
        publishRawBytes(topic, "undecodable", new byte[] {1, 2, 3, 4, 5});
        publish(topic, lifecycle("pd-good", 1, RuleState.ACTIVE, BANDS, 1_000L));

        RiskRuleRegistry registry = new RiskRuleRegistry(NO_BOOTSTRAP);
        List<String> rejections = new CopyOnWriteArrayList<>();

        try (RuleTimelineLoader loader = loader(topic, registry, Duration.ofSeconds(30), rejections)) {
            // The availability assertion for a decode failure, distinct from the one above for a
            // validation failure. A SerializationException is thrown by the consumer inside
            // poll() itself, before any ConsumerRecords are returned, so it must never propagate
            // out of loadInitialSnapshot() and fail startup.
            loader.loadInitialSnapshot();

            assertThat(loader.isLoaded()).isTrue();
            assertThat(registry.inForceAt("price-deviation", 2_000L)).singleElement()
                    .satisfies(rule -> assertThat(rule.ruleId()).isEqualTo("pd-good"));
            // malformed_record, not null_value: the two must stay distinguishable because they
            // become separate metric labels in Task 10.
            assertThat(rejections).containsExactly("malformed_record");
        }
    }

    @Test
    void a_rejected_version_leaves_the_previously_in_force_version_alone() {
        String topic = freshTopic();
        publish(topic,
                lifecycle("pd", 1, RuleState.ACTIVE, BANDS, 1_000L),
                lifecycle("pd", 2, RuleState.ACTIVE,
                        Map.of("warn-deviation-percent", "not a number",
                                "critical-deviation-percent", "5.0"), 3_000L));

        RiskRuleRegistry registry = new RiskRuleRegistry(NO_BOOTSTRAP);
        List<String> rejections = new CopyOnWriteArrayList<>();

        try (RuleTimelineLoader loader = loader(topic, registry, Duration.ofSeconds(30), rejections)) {
            loader.loadInitialSnapshot();

            // v2 never enters the timeline, so v1 is still what a trade at 4000 evaluates against.
            // Falling back to no rule at all would silently stop risk evaluation on a typo.
            assertThat(registry.inForceAt("price-deviation", 4_000L)).singleElement()
                    .satisfies(rule -> assertThat(rule.version()).isEqualTo(1L));
            assertThat(rejections).containsExactly("unparseable_value");
        }
    }

    @Test
    void the_load_fails_startup_when_it_cannot_reach_the_end_offsets_in_time() {
        String topic = freshTopic();
        for (int i = 1; i <= 2000; i++) {
            publish(topic, lifecycle("pd-" + i, 1, RuleState.ACTIVE, BANDS, 1_000L));
        }

        RiskRuleRegistry registry = new RiskRuleRegistry(NO_BOOTSTRAP);
        try (RuleTimelineLoader loader = loader(topic, registry, Duration.ofMillis(1),
                new CopyOnWriteArrayList<>())) {
            // Releasing the listener with a partial fold would evaluate trades against the
            // ungoverned bootstrap set while a governed version was in force. That is a wrong
            // verdict rather than a delayed one, so the service fails to start instead.
            assertThatExceptionOfType(IllegalStateException.class)
                    .isThrownBy(loader::loadInitialSnapshot)
                    .withMessageContaining("Timed out");
        }
    }
}
