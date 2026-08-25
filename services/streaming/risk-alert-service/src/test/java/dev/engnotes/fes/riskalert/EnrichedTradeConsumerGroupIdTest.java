package dev.engnotes.fes.riskalert;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class EnrichedTradeConsumerGroupIdTest {

    @Test
    void the_listener_id_does_not_override_the_configured_consumer_group() throws Exception {
        Method consume = EnrichedTradeConsumer.class.getMethod(
                "consume", org.apache.kafka.clients.consumer.ConsumerRecord.class,
                org.springframework.kafka.support.Acknowledgment.class);
        KafkaListener listener = consume.getAnnotation(KafkaListener.class);

        // Spring Kafka's idIsGroup defaults to true, so an explicit id silently replaces the
        // configured group.id. Left at the default, this service would join a group named after the
        // listener rather than risk-alert-service, which is the only name the committed Kafka policy
        // authorizes, and every functional test would still pass.
        assertThat(listener.idIsGroup()).isFalse();
        assertThat(listener.id()).isEqualTo(EnrichedTradeConsumer.LISTENER_ID);
    }
}
