package dev.engnotes.fes.common.kafka;

import java.util.Map;

/**
 * The SASL client properties {@link KafkaSecurityConfiguration} derives, exposed as a bean so code
 * that builds its own {@code KafkaConsumer} or {@code KafkaProducer} outside a Spring Boot factory
 * can still authenticate under the {@code secure-kafka} profile.
 *
 * <p>{@link KafkaSecurityConfiguration}'s customizers reach only the {@code ConsumerFactory} and
 * {@code ProducerFactory} beans Spring Boot autoconfigures for {@code @KafkaListener} and
 * {@code KafkaTemplate}. A consumer built by hand from a raw properties map, such as one that
 * {@code assign()}s partitions instead of joining a group, never passes through either factory and
 * would otherwise connect as PLAINTEXT regardless of profile. This bean only exists under
 * {@code secure-kafka} (see {@link KafkaSecurityConfiguration}), so a caller must treat its absence
 * as the dev profile and inject it optionally rather than requiring it.
 *
 * <p>The identity is still derived, never configured (ADR-031): this class carries the same map
 * {@link KafkaSecurityConfiguration} already built from {@code spring.application.name}, not a
 * second computation of it, so a module cannot state a JAAS config of its own by depending on this
 * bean instead.
 */
public record KafkaSaslProfile(Map<String, Object> properties) {
}
