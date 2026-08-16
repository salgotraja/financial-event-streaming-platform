package dev.engnotes.fes.testing;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The authorization contract every write-only ingestion identity must satisfy: one allowed action
 * and two explicitly denied ones (NFR-05.19).
 *
 * <p>The denials are the point. Each producer's row in {@code identity-trust-matrix.md} asserts that
 * it cannot read the topic it writes and cannot write anyone else's, and a row that nothing tests is
 * a description rather than a control.
 *
 * <p>What this proves is the committed policy in the service's own {@code kafka-acls.yml}, applied
 * to a real broker that denies by default. It does not prove the deployed service runs as that
 * identity; that binding belongs to the strict-security stack and the cloud policies.
 *
 * <p>The read denial uses {@code assign} rather than {@code subscribe} on purpose. Subscribing needs
 * a group, and an identity with no group grant would be refused for the group before the broker
 * considered the topic, which would prove something other than what the row claims.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class KafkaProducerAuthorizationContract {

    /** The workload identity under test, matching the principal in its committed policy. */
    protected abstract String principal();

    /** The single topic this identity may write. */
    protected abstract String ownTopic();

    /** A topic belonging to another service, which this identity must not be able to write. */
    protected abstract String foreignTopic();

    @BeforeAll
    void applyCommittedPolicy() {
        SecureKafkaStack.start();
        KafkaAclPolicy policy = KafkaAclPolicy.load();
        assertThat(policy.principal())
                .as("the test and the committed policy must name the same identity")
                .isEqualTo(principal());
        SecureKafkaStack.apply(policy);
        SecureKafkaStack.createTopics(foreignTopic());
    }

    @Test
    @DisplayName("should allow writing the topic this identity owns")
    void should_allow_writing_the_topic_this_identity_owns() throws Exception {
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(principal()))) {
            var metadata = producer.send(new ProducerRecord<>(ownTopic(), "K", new byte[]{1})).get();
            assertThat(metadata.topic()).isEqualTo(ownTopic());
        }
    }

    @Test
    @DisplayName("should deny reading the topic this identity writes")
    void should_deny_reading_the_topic_this_identity_writes() {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(principal(), null))) {
            consumer.assign(List.of(new TopicPartition(ownTopic(), 0)));

            assertThatThrownBy(() -> consumer.poll(Duration.ofSeconds(10)))
                    .as("a write-only identity that can read back its own topic is not write-only")
                    .isInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    @DisplayName("should deny writing another service's topic")
    void should_deny_writing_another_services_topic() {
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(principal()))) {
            assertThatThrownBy(() ->
                    producer.send(new ProducerRecord<>(foreignTopic(), "K", new byte[]{1})).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }
}
