package dev.engnotes.fes.riskalert;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.RiskRuleLifecycleEvent;
import dev.engnotes.fes.events.RuleState;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.testing.KafkaAvroStack;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The listener against a real broker and a real Schema Registry.
 *
 * <p>Every topic is unique to this class, including the output topic, so another module's tests
 * cannot race these assertions.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
@DisplayName("EnrichedTradeConsumer against a real broker and registry")
class EnrichedTradeConsumerIntegrationTest {

    private static final String TRADE_TOPIC = "ras-it-" + UUID.randomUUID();
    private static final String RULE_TOPIC = "ras-rules-it-" + UUID.randomUUID();
    private static final String OUTPUT_TOPIC = "ras-out-it-" + UUID.randomUUID();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        // RuleTimelineLoader assigns partitions directly rather than subscribing, so the rule
        // topic must exist before the context starts: see RiskAlertTestKafka.createTopic.
        // RiskAlertTestKafka.alertConsumer does the same for the output topic, so it is created
        // here too rather than left to auto-creation.
        RiskAlertTestKafka.createTopic(TRADE_TOPIC, 1);
        RiskAlertTestKafka.createTopic(RULE_TOPIC, 6);
        RiskAlertTestKafka.createTopic(OUTPUT_TOPIC, 1);
        // application.yml sets auto.register.schemas: false for this service's own producer, so
        // the output subject must already exist before the service's first send: see
        // RiskAlertTestKafka.registerSchema.
        RiskAlertTestKafka.registerSchema(OUTPUT_TOPIC, RiskAlertEvent.getClassSchema());
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                KafkaAvroStack::schemaRegistryUrl);
        registry.add("fes.risk-alert-service.topic", () -> TRADE_TOPIC);
        registry.add("fes.risk-alert-service.rule-topic", () -> RULE_TOPIC);
        registry.add("fes.risk-alert-service.output-topic", () -> OUTPUT_TOPIC);
    }

    @Test
    void a_breaching_trade_produces_an_alert_through_a_real_broker_and_registry() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, RiskAlertEvent> consumer =
                     RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            producer.send(new ProducerRecord<>(TRADE_TOPIC, "RELIANCE",
                    RiskAlertTestKafka.trade("trade-breach", "RELIANCE", 6.0, Instant.ofEpochMilli(1_000L))));
            producer.flush();

            RiskAlertEvent alert =
                    RiskAlertTestKafka.drain(consumer, 1, Duration.ofSeconds(30)).getFirst();

            assertThat(alert.getAlertType()).isEqualTo(dev.engnotes.fes.events.AlertType.PRICE_DEVIATION);
            assertThat(alert.getSeverity()).isEqualTo(Severity.CRITICAL);
            assertThat(alert.getTriggeringTradeId()).hasToString("trade-breach");
            // Version 0 is the ungoverned bootstrap set from application.yml. Nothing writes
            // risk-rules.events until Phase 5, so this is the honest value rather than a fiction.
            assertThat(alert.getRuleId()).hasToString("price-deviation");
            assertThat(alert.getRuleVersion()).isZero();
        }
    }

    @Test
    void a_non_breaching_trade_produces_nothing() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, RiskAlertEvent> consumer =
                     RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            producer.send(new ProducerRecord<>(TRADE_TOPIC, "TCS",
                    RiskAlertTestKafka.trade("trade-quiet", "TCS", 0.5, Instant.ofEpochMilli(1_000L))));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "TCS",
                    RiskAlertTestKafka.trade("trade-loud", "TCS", 6.0, Instant.ofEpochMilli(1_100L))));
            producer.flush();

            // The second trade is the tracer. Asserting only that nothing arrives for the first
            // would pass even if the listener were never started at all, which is exactly the shape
            // of test this project has already had to rewrite four times.
            List<RiskAlertEvent> alerts = RiskAlertTestKafka.drain(consumer, 1, Duration.ofSeconds(30));

            assertThat(alerts).singleElement()
                    .satisfies(alert -> assertThat(alert.getTriggeringTradeId())
                            .hasToString("trade-loud"));
        }
    }

    /**
     * Isolated in its own nested context, with its own topic set. {@code risk-rules.events} is
     * not compacted and {@code RiskRuleRegistry} suppresses the bootstrap for a rule type the
     * moment any governed version of it exists, permanently for the life of whatever registry
     * folded it, so the {@code pd-tight} record this test publishes would outrank the bootstrap
     * for every other test in this class if they shared its rule topic or its Spring context,
     * regardless of JUnit's method order, which is deliberately not guaranteed. A distinct
     * {@code @DynamicPropertySource} here gives this nested class its own merged context
     * configuration, and therefore its own {@code RuleTimelineLoader} and {@code RiskRuleRegistry}
     * reading its own topics, so the governed version it writes can never reach the other four
     * tests' rule topic, trade topic or output topic.
     */
    @Nested
    @DisplayName("a governed rule version published to the rule topic")
    class GovernedRuleVersion {

        private static final String TRADE_TOPIC = "ras-it-governed-" + UUID.randomUUID();
        private static final String RULE_TOPIC = "ras-rules-it-governed-" + UUID.randomUUID();
        private static final String OUTPUT_TOPIC = "ras-out-it-governed-" + UUID.randomUUID();

        // Must be static: Spring requires @DynamicPropertySource methods to be static even inside
        // a @Nested (non-static, JUnit-mandated) test class; Java has allowed static members in
        // inner classes since JDK 16, which is what makes this legal here.
        @DynamicPropertySource
        static void governedProperties(DynamicPropertyRegistry registry) {
            RiskAlertTestKafka.createTopic(TRADE_TOPIC, 1);
            RiskAlertTestKafka.createTopic(RULE_TOPIC, 6);
            RiskAlertTestKafka.createTopic(OUTPUT_TOPIC, 1);
            RiskAlertTestKafka.registerSchema(OUTPUT_TOPIC, RiskAlertEvent.getClassSchema());
            registry.add("fes.risk-alert-service.topic", () -> TRADE_TOPIC);
            registry.add("fes.risk-alert-service.rule-topic", () -> RULE_TOPIC);
            registry.add("fes.risk-alert-service.output-topic", () -> OUTPUT_TOPIC);
        }

        @Test
        void changes_the_verdict() {
            try (KafkaProducer<String, RiskRuleLifecycleEvent> rules = RiskAlertTestKafka.producer();
                 KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
                 KafkaConsumer<String, RiskAlertEvent> consumer =
                         RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

                rules.send(new ProducerRecord<>(RULE_TOPIC, "pd-tight",
                        RiskRuleLifecycleEvent.newBuilder()
                                .setRuleId("pd-tight")
                                .setVersion(9L)
                                .setState(RuleState.ACTIVE)
                                .setRuleType("price-deviation")
                                .setParameters(Map.of("warn-deviation-percent", "0.1",
                                        "critical-deviation-percent", "0.2"))
                                .setMakerSubject("maker@fes.local")
                                .setCheckerSubject("checker@fes.local")
                                .setReason("tighten the band")
                                .setEffectiveAt(Instant.ofEpochMilli(1_000L))
                                .setEventTimestamp(Instant.ofEpochMilli(1_000L))
                                .setCorrelationId(UUID.randomUUID().toString())
                                .build()));
                rules.flush();

                // 0.5 percent is under the bootstrap's 2.0 warning band and over the governed
                // rule's 0.2 critical band, so this trade alerts only if the governed version
                // actually reached the running service. That makes this the end-to-end proof of
                // the whole increment, and the assertions inside this block are also what would
                // fail if the readiness gate ever stopped opening: openTheReadinessGate is the
                // only thing that calls RuleTimelineLoader.loadInitialSnapshot(), and that call is
                // what starts both the catch-up fold and the follower thread that keeps folding
                // risk-rules.events afterwards. startTheTradeListenerOnceLoaded has no dependency
                // on that bean, so a stripped @Bean there would still let the trade listener start
                // against an empty registry. This governed-version pd-tight record would then
                // never reach the registry, the trade would never breach the bootstrap's own 2.0
                // percent band at 0.5 percent, and assertThat(records).isNotEmpty() would time out
                // after 30 seconds instead of ever seeing pd-tight at version 9.
                Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    producer.send(new ProducerRecord<>(TRADE_TOPIC, "INFY",
                            RiskAlertTestKafka.trade("trade-governed-" + UUID.randomUUID(), "INFY", 0.5,
                                    Instant.ofEpochMilli(2_000L))));
                    producer.flush();

                    ConsumerRecords<String, RiskAlertEvent> records = consumer.poll(Duration.ofMillis(500));
                    assertThat(records).isNotEmpty();
                    records.forEach(record -> {
                        assertThat(record.value().getRuleId()).hasToString("pd-tight");
                        assertThat(record.value().getRuleVersion()).isEqualTo(9L);
                        assertThat(record.value().getSeverity()).isEqualTo(Severity.CRITICAL);
                    });
                });
            }
        }
    }

    @Test
    void redelivering_the_same_trade_produces_the_same_alert_id() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, RiskAlertEvent> consumer =
                     RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            EnrichedTradeEvent repeated =
                    RiskAlertTestKafka.trade("trade-redelivered", "HDFCBANK", 6.0, Instant.ofEpochMilli(1_000L));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "HDFCBANK", repeated));
            producer.send(new ProducerRecord<>(TRADE_TOPIC, "HDFCBANK", repeated));
            producer.flush();

            List<RiskAlertEvent> alerts = RiskAlertTestKafka.drain(consumer, 2, Duration.ofSeconds(30));

            // At-least-once makes redelivery normal (ADR-019), and this is what lets a downstream
            // consumer tell one breach delivered twice from two distinct breaches. A randomUUID
            // alertId would pass every other test in this class and fail only here.
            assertThat(alerts.get(0).getAlertId()).isEqualTo(alerts.get(1).getAlertId());
        }
    }

    @Test
    void the_trace_headers_survive_the_hop_onto_the_alert_topic() {
        try (KafkaProducer<String, EnrichedTradeEvent> producer = RiskAlertTestKafka.producer();
             KafkaConsumer<String, RiskAlertEvent> consumer =
                     RiskAlertTestKafka.alertConsumer(OUTPUT_TOPIC)) {

            ProducerRecord<String, EnrichedTradeEvent> record = new ProducerRecord<>(TRADE_TOPIC,
                    "WIPRO", RiskAlertTestKafka.trade("trade-traced", "WIPRO", 6.0, Instant.ofEpochMilli(1_000L)));
            record.headers().add("traceparent",
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".getBytes());
            producer.send(record);
            producer.flush();

            // NFR-04.1 requires end-to-end tracing across services that may never deserialise the
            // body, so traceparent has to survive on the headers rather than only in the payload.
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, RiskAlertEvent> records = consumer.poll(Duration.ofMillis(500));
                assertThat(records).isNotEmpty();
                records.forEach(delivered ->
                        assertThat(delivered.headers().lastHeader("traceparent")).isNotNull());
            });
        }
    }
}
