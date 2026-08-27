package dev.engnotes.fes.riskalert;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import dev.engnotes.fes.testing.KafkaAclPolicy;
import dev.engnotes.fes.testing.SecureKafkaStack;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The risk-alert-service identity's authorization contract: one allowed action and three denied
 * ones (NFR-05.19).
 *
 * <p>There is no grant on risk-rules.events to prove an allow for here, deliberately. That topic is
 * the control plane's governance record, and this identity only ever reads it; the read grant is
 * exercised by {@code RiskAlertServiceIdentityStackTest} and by the governed-rule integration test,
 * not restated here as a third allow.
 *
 * <p>The denial that matters most is writing risk-rules.events. The entire justification for
 * {@code RuleTimelineLoader} logging and skipping a malformed or invalid governed version, rather
 * than dead-lettering it, is that this identity holds no write grant there: a streaming workload
 * that could write its own governance record could manufacture the approved rule version it then
 * evaluates against, which defeats maker-checker entirely. The other two denials are the general
 * shape every consuming, producing identity in this repository must prove: it cannot write the
 * topic it consumes, and it cannot join another workload's consumer group.
 */
@DisplayName("risk-alert-service workload identity")
class RiskAlertServiceAuthorizationTest {

    private static final String PRINCIPAL = "risk-alert-service";
    private static final String CONSUMED_TOPIC = "trades.enriched";
    private static final String PRODUCED_TOPIC = "notifications.alerts";
    private static final String RULE_TOPIC = "risk-rules.events";
    private static final String OWN_GROUP = "risk-alert-service";
    private static final String FOREIGN_GROUP = "trade-enrichment-service";

    @BeforeAll
    static void applyCommittedPolicy() {
        SecureKafkaStack.start();
        KafkaAclPolicy policy = KafkaAclPolicy.load();
        assertThat(policy.principal()).isEqualTo(PRINCIPAL);
        SecureKafkaStack.apply(policy);
        SecureKafkaStack.seed(CONSUMED_TOPIC, "RELIANCE", new byte[]{0, 0, 0, 0, 1, 42});
    }

    @Test
    @DisplayName("should allow reading trades.enriched and writing notifications.alerts")
    void should_allow_reading_trades_enriched_and_writing_notifications_alerts() throws Exception {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(CONSUMED_TOPIC));

            ConsumerRecords<String, byte[]> records = poll(consumer);

            assertThat(records.count())
                    .as("the service must be able to read the trades it evaluates")
                    .isPositive();
        }

        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            producer.send(new ProducerRecord<>(PRODUCED_TOPIC, "K", new byte[]{1})).get();
        }
    }

    @Test
    @DisplayName("should deny writing the governed rule topic")
    void should_deny_writing_the_governed_rule_topic() {
        // The assertion that matters most. risk-rules.events is the control plane's governance
        // record, and RuleTimelineLoader logs and skips a malformed or invalid governed version
        // rather than dead-lettering it precisely because this identity can never write here: a
        // grant on this topic would let a streaming workload manufacture the approved rule version
        // it then evaluates against, defeating maker-checker entirely.
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            assertThatThrownBy(() ->
                    producer.send(new ProducerRecord<>(RULE_TOPIC, "K", new byte[]{1})).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    @DisplayName("should deny writing the topic it consumes")
    void should_deny_writing_the_topic_it_consumes() {
        // A service that can write its own input can replay its own backlog.
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            assertThatThrownBy(() ->
                    producer.send(new ProducerRecord<>(CONSUMED_TOPIC, "K", new byte[]{1})).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    @DisplayName("should deny joining a consumer group other than its own")
    void should_deny_joining_a_consumer_group_other_than_its_own() {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, FOREIGN_GROUP))) {
            consumer.subscribe(List.of(CONSUMED_TOPIC));

            assertThatThrownBy(() -> poll(consumer))
                    .as("joining another workload's group would let this identity advance its offsets")
                    .isInstanceOf(GroupAuthorizationException.class);
        }
    }

    private static ConsumerRecords<String, byte[]> poll(KafkaConsumer<String, byte[]> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records;
            }
        }
        return ConsumerRecords.empty();
    }
}
