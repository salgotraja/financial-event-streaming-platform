package dev.engnotes.fes.marketdatacacheprojector;

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
 * The projector identity's authorization contract: one allowed action and two denied ones
 * (NFR-05.19).
 *
 * <p>The denials that matter for a projector are reading a stream it has no business in, and
 * joining another workload's consumer group. An identity that can join another group can advance
 * that workload's offsets and make its records disappear without touching a topic ACL.
 */
@DisplayName("market-data-cache-projector workload identity")
class MarketDataCacheProjectorAuthorizationTest {

    private static final String PRINCIPAL = "market-data-cache-projector";
    private static final String PROJECTED_TOPIC = "market-data.ticks";
    private static final String FOREIGN_TOPIC = "trades.raw";
    private static final String OWN_GROUP = "market-data-cache-projector";
    private static final String FOREIGN_GROUP = "trade-enrichment-service";

    @BeforeAll
    static void applyCommittedPolicy() {
        SecureKafkaStack.start();
        KafkaAclPolicy policy = KafkaAclPolicy.load();
        assertThat(policy.principal()).isEqualTo(PRINCIPAL);
        SecureKafkaStack.apply(policy);
        SecureKafkaStack.seed(PROJECTED_TOPIC, "RELIANCE", new byte[]{0, 0, 0, 0, 1, 42});
    }

    @Test
    @DisplayName("should allow reading market-data.ticks through its own consumer group")
    void should_allow_reading_market_data_ticks_through_its_own_consumer_group() {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(PROJECTED_TOPIC));

            ConsumerRecords<String, byte[]> records = poll(consumer);

            assertThat(records.count())
                    .as("the projector must be able to read the stream it projects")
                    .isPositive();
        }
    }

    @Test
    @DisplayName("should deny reading the trade stream")
    void should_deny_reading_the_trade_stream() {
        // Its only READ grant is market-data.ticks. A cache projector that can read the trade flow
        // holds access it has no use for, and every such grant is one an attacker inherits.
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, OWN_GROUP))) {
            consumer.subscribe(List.of(FOREIGN_TOPIC));

            assertThatThrownBy(() -> poll(consumer))
                    .isInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    @DisplayName("should deny joining a consumer group other than its own")
    void should_deny_joining_a_consumer_group_other_than_its_own() {
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(SecureKafkaStack.consumerConfig(PRINCIPAL, FOREIGN_GROUP))) {
            consumer.subscribe(List.of(PROJECTED_TOPIC));

            assertThatThrownBy(() -> poll(consumer))
                    .as("joining another workload's group would let this identity advance its offsets")
                    .isInstanceOf(GroupAuthorizationException.class);
        }
    }

    @Test
    @DisplayName("should deny writing the topic it projects")
    void should_deny_writing_the_topic_it_projects() {
        // Its only WRITE grant is the .dlq topic. A projector that can write market-data.ticks can
        // manufacture the prices it is supposed to be observing.
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(SecureKafkaStack.producerConfig(PRINCIPAL))) {
            assertThatThrownBy(() ->
                    producer.send(new ProducerRecord<>(PROJECTED_TOPIC, "K", new byte[]{1})).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
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
