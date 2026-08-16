package dev.engnotes.fes.audit;

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
 * The audit identity's authorization contract: one allowed action and two denied ones (NFR-05.19).
 *
 * <p>It does not extend the producer contract because its shape is the inverse. The archival
 * consumer reads the evidence topics and writes only quarantine, so the interesting denials are a
 * write onto a financial topic, which `specification-v1.2.md` rules out as "no arbitrary topic
 * writes", and joining a consumer group that is not its own.
 *
 * <p>Group scoping is the denial no producer can provide, because no producer has a group. An
 * identity that can join another workload's group can advance that workload's offsets and make its
 * records disappear without touching a topic ACL.
 */
@DisplayName("audit-service workload identity")
class AuditServiceAuthorizationTest {

    private static final String PRINCIPAL = "audit-service";
    private static final String ARCHIVED_TOPIC = "trades.raw";
    private static final String OWN_GROUP = "audit-service";
    private static final String FOREIGN_GROUP = "trade-enrichment-service";

    @BeforeAll
    static void applyCommittedPolicy() {
        SecureKafkaStack.start();
        KafkaAclPolicy policy = KafkaAclPolicy.load();
        assertThat(policy.principal()).isEqualTo(PRINCIPAL);
        SecureKafkaStack.apply(policy);
        SecureKafkaStack.seed(ARCHIVED_TOPIC, "RELIANCE", new byte[]{0, 0, 0, 0, 1, 42});
    }

    @Test
    @DisplayName("should allow reading an archived topic through its own consumer group")
    void should_allow_reading_an_archived_topic_through_its_own_consumer_group() {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(ARCHIVED_TOPIC));

            ConsumerRecords<String, byte[]> records = poll(consumer);

            assertThat(records.count())
                    .as("the archive must be able to read the evidence it exists to keep")
                    .isPositive();
        }
    }

    @Test
    @DisplayName("should deny writing a financial topic")
    void should_deny_writing_a_financial_topic() {
        // Its only WRITE grants are the .dlq topics. An audit identity that can write trades.raw can
        // manufacture the evidence it is supposed to be archiving.
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            assertThatThrownBy(() ->
                    producer.send(new ProducerRecord<>(ARCHIVED_TOPIC, "K", new byte[]{1})).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    @DisplayName("should deny joining a consumer group other than its own")
    void should_deny_joining_a_consumer_group_other_than_its_own() {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, FOREIGN_GROUP))) {
            consumer.subscribe(List.of(ARCHIVED_TOPIC));

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
