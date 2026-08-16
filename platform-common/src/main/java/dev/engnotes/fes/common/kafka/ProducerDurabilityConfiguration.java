package dev.engnotes.fes.common.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

/**
 * The production durability profile, applied to every producer in the platform.
 *
 * <p>These values are not tuning preferences. {@code acks=all} with {@code min.insync.replicas=2}
 * and replication factor 3 is what makes broker loss survivable without message loss (NFR-02.2), and
 * {@code enable.idempotence=true} is what makes the at-least-once contract hold across producer
 * retries without duplicating within a session (ADR-019).
 *
 * <p>Applied programmatically rather than left to each service's YAML so a service cannot silently
 * ship with weaker durability. A load test may override these for a clearly labelled non-durable
 * ceiling experiment; that result never stands in for the production profile.
 *
 * <p>Deliberately absent: any transactional configuration. The platform is at-least-once with
 * deduplication by deterministic key, never exactly-once (ADR-019).
 */
@Configuration(proxyBeanMethods = false)
public class ProducerDurabilityConfiguration {

    static final Map<String, Object> DURABILITY_PROFILE = Map.of(
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5,
            ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000,
            ProducerConfig.BATCH_SIZE_CONFIG, 65_536,
            ProducerConfig.LINGER_MS_CONFIG, 5,
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4",
            ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864L);

    @Bean
    DefaultKafkaProducerFactoryCustomizer durabilityCustomizer() {
        return this::applyTo;
    }

    void applyTo(DefaultKafkaProducerFactory<?, ?> factory) {
        factory.updateConfigs(DURABILITY_PROFILE);
    }

    /** Exposed for assertions and for load-test harnesses that need to state what they overrode. */
    public static Map<String, Object> durabilityProfile() {
        return DURABILITY_PROFILE;
    }
}
