package dev.engnotes.fes.tradeenrichment;

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
 * The trade-enrichment-service identity's authorization contract: three allowed actions and three
 * denied ones (NFR-05.19).
 *
 * <p>The denial that matters most is market-data.ticks. Market state reaches this service through
 * Redis, and a grant on the tick topic would be a second route to the same data and a way to bypass
 * the projection ADR-027 exists to enforce. The other two denials are the general shape every
 * consuming, producing identity in this repository must prove: it cannot write the topic it
 * consumes, and it cannot join another workload's consumer group.
 */
@DisplayName("trade-enrichment-service workload identity")
class TradeEnrichmentServiceAuthorizationTest {

    private static final String PRINCIPAL = "trade-enrichment-service";
    private static final String CONSUMED_TOPIC = "trades.raw";
    private static final String REFERENCE_TOPIC = "reference-data.instruments";
    private static final String PRODUCED_TOPIC = "trades.enriched";
    private static final String FORBIDDEN_TOPIC = "market-data.ticks";
    private static final String OWN_GROUP = "trade-enrichment-service";
    private static final String FOREIGN_GROUP = "market-data-cache-projector";

    @BeforeAll
    static void applyCommittedPolicy() {
        SecureKafkaStack.start();
        KafkaAclPolicy policy = KafkaAclPolicy.load();
        assertThat(policy.principal()).isEqualTo(PRINCIPAL);
        SecureKafkaStack.apply(policy);
        SecureKafkaStack.seed(CONSUMED_TOPIC, "RELIANCE", new byte[]{0, 0, 0, 0, 1, 42});
        SecureKafkaStack.seed(REFERENCE_TOPIC, "INST-1", new byte[]{0, 0, 0, 0, 1, 42});
        SecureKafkaStack.seed(FORBIDDEN_TOPIC, "RELIANCE", new byte[]{0, 0, 0, 0, 1, 42});
    }

    @Test
    @DisplayName("should allow reading trades.raw through its own consumer group")
    void should_allow_reading_trades_raw_through_its_own_consumer_group() {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(CONSUMED_TOPIC));

            ConsumerRecords<String, byte[]> records = poll(consumer);

            assertThat(records.count())
                    .as("the service must be able to read the trades it enriches")
                    .isPositive();
        }
    }

    @Test
    @DisplayName("should allow reading the instrument master")
    void should_allow_reading_the_instrument_master() {
        // No group id: reference-data.instruments is consumed by an assign()-ed consumer that joins
        // no group, so this proves the topic grant without relying on a group ACL this identity does
        // not hold for it.
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, null))) {
            consumer.assign(consumer.partitionsFor(REFERENCE_TOPIC).stream()
                    .map(info -> new org.apache.kafka.common.TopicPartition(
                            info.topic(), info.partition()))
                    .toList());
            consumer.seekToBeginning(consumer.assignment());

            ConsumerRecords<String, byte[]> records = poll(consumer);

            assertThat(records.count())
                    .as("the service must be able to fold the instrument master")
                    .isPositive();
        }
    }

    @Test
    @DisplayName("should allow writing trades.enriched")
    void should_allow_writing_trades_enriched() throws Exception {
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            producer.send(new ProducerRecord<>(PRODUCED_TOPIC, "K", new byte[]{1})).get();
        }
    }

    @Test
    @DisplayName("should deny reading the market data tick stream")
    void should_deny_reading_the_market_data_tick_stream() {
        // The assertion that matters most. Market state comes from Redis, and a grant here would be
        // a second route to the same data.
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(FORBIDDEN_TOPIC));

            assertThatThrownBy(() -> poll(consumer))
                    .isInstanceOf(TopicAuthorizationException.class);
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
