package dev.engnotes.fes.positionexposure;

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
 * The position-exposure-service identity's authorization contract: one allowed action and three
 * denied ones (NFR-05.19).
 *
 * <p>There is no READ grant on positions.snapshots. This service writes that topic and never reads
 * it; rebuilding the read model replays trades.enriched, not its own output, so a read grant here
 * would widen the identity for a path that does not exist (FR-11.5, increment 2). The other two
 * denials are the general shape every consuming, producing identity in this repository must prove:
 * it cannot write the topic it consumes, and it cannot join another workload's consumer group.
 */
@DisplayName("position-exposure-service workload identity")
class PositionExposureServiceAuthorizationTest {

    private static final String PRINCIPAL = "position-exposure-service";
    private static final String CONSUMED_TOPIC = "trades.enriched";
    private static final String PRODUCED_TOPIC = "positions.snapshots";
    private static final String OWN_GROUP = "position-exposure-service";
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
    @DisplayName("should allow reading trades.enriched and writing positions.snapshots")
    void should_allow_reading_trades_enriched_and_writing_positions_snapshots() throws Exception {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(CONSUMED_TOPIC));

            ConsumerRecords<String, byte[]> records = poll(consumer);

            assertThat(records.count())
                    .as("the service must be able to read the trades it projects")
                    .isPositive();
        }

        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            producer.send(new ProducerRecord<>(PRODUCED_TOPIC, "K", new byte[]{1})).get();
        }
    }

    @Test
    void should_deny_writing_the_topic_it_consumes() {
        // A read model that could write trades.enriched could manufacture the trades it reports.
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            assertThatThrownBy(() ->
                    producer.send(new ProducerRecord<>(CONSUMED_TOPIC, "K", new byte[]{1})).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    void should_deny_reading_the_topic_it_writes() {
        // Write-only on its own output: nothing in this service reads positions.snapshots, and the
        // increment 2 rebuild replays trades.enriched rather than this topic.
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(PRODUCED_TOPIC));

            assertThatThrownBy(() -> poll(consumer))
                    .isInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
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
