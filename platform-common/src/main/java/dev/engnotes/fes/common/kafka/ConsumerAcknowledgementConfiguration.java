package dev.engnotes.fes.common.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

/**
 * The offset-management profile, applied to every consumer in the platform.
 *
 * <p>Auto-commit is the single setting that turns at-least-once into at-most-once by accident: the
 * consumer commits on a timer, so a crash between the commit and the end of processing loses the
 * record with no error anywhere. The platform commits explicitly after successful processing
 * instead (ADR-019), which is why {@code enable.auto.commit=false} and
 * {@link AckMode#MANUAL_IMMEDIATE} are forced here rather than left to each service's YAML. A
 * service that omits them still gets them.
 *
 * <p>{@code isolation.level} is deliberately not set. Leaving the Kafka default
 * {@code read_uncommitted} is the honest configuration: no producer uses the transactional API, and
 * {@code read_committed} would advertise a write path that does not exist (ADR-019).
 */
@Configuration(proxyBeanMethods = false)
public class ConsumerAcknowledgementConfiguration {

    static final Map<String, Object> OFFSET_PROFILE = Map.of(
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    @Bean
    DefaultKafkaConsumerFactoryCustomizer explicitOffsetCustomizer() {
        return this::applyTo;
    }

    void applyTo(DefaultKafkaConsumerFactory<?, ?> factory) {
        factory.updateConfigs(OFFSET_PROFILE);
    }

    /**
     * Boot's annotation-driven listener configuration applies a single {@code ContainerCustomizer}
     * bean to every container it builds, after the properties are mapped, so this wins over a
     * {@code spring.kafka.listener.ack-mode} that says something weaker.
     */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> manualAckCustomizer() {
        return container -> container.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    }

    /** Exposed for assertions that a service did not weaken offset management. */
    public static Map<String, Object> offsetProfile() {
        return OFFSET_PROFILE;
    }
}
