package dev.engnotes.fes.common.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Consumer offset management applied by platform-common")
class ConsumerAcknowledgementConfigurationTest {

    private final ConsumerAcknowledgementConfiguration configuration =
            new ConsumerAcknowledgementConfiguration();

    @Test
    @DisplayName("should disable auto commit even when a service configured it on")
    void should_disable_auto_commit_even_when_a_service_configured_it_on() {
        Map<String, Object> serviceConfig = new HashMap<>();
        serviceConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(serviceConfig);

        configuration.applyTo(factory);

        assertThat(factory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    @Test
    @DisplayName("should not set isolation level, because no producer is transactional")
    void should_not_set_isolation_level_because_no_producer_is_transactional() {
        DefaultKafkaConsumerFactory<String, String> factory =
                new DefaultKafkaConsumerFactory<>(new HashMap<>());

        configuration.applyTo(factory);

        assertThat(factory.getConfigurationProperties())
                .doesNotContainKey(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
    }

    @Test
    @DisplayName("should force manual immediate acknowledgement over a weaker configured ack mode")
    void should_force_manual_immediate_acknowledgement_over_a_weaker_configured_ack_mode() {
        ContainerProperties containerProperties = new ContainerProperties("any-topic");
        containerProperties.setAckMode(AckMode.BATCH);
        ConcurrentMessageListenerContainer<Object, Object> container = new ConcurrentMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<>(new HashMap<>()), containerProperties);

        configuration.manualAckCustomizer().configure(container);

        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(AckMode.MANUAL_IMMEDIATE);
    }
}
