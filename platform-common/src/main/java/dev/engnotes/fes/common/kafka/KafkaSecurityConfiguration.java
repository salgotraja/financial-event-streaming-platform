package dev.engnotes.fes.common.kafka;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * How a service authenticates to Kafka, applied to the producer and consumer factories Spring Boot
 * autoconfigures (ADR-031). A Kafka Streams service will need the equivalent applied to its
 * {@code StreamsBuilderFactoryBean} once one exists; nothing here reaches it yet.
 *
 * <p><strong>The username is derived, never configured.</strong> It is
 * {@code spring.application.name}, and no service module holds a property that names an identity.
 * The failure this guards against is mundane: a service handed an administrator's credential, which
 * on a local broker is a super user, would pass every functional test in the repository while
 * defeating every ACL at once. Derived, that service still presents its own username and the broker
 * rejects it.
 *
 * <p>The secret, the bootstrap servers and the security protocol come from the environment, because
 * those are facts about a deployment. The identity is not.
 *
 * <p>This takes the shape {@link ProducerDurabilityConfiguration} already takes and for the same
 * reason: what the platform must guarantee is imposed by shared code rather than restated in five
 * service configurations that are each free to drift.
 *
 * <p>Active only under the {@code secure-kafka} profile, so a developer stack with a plaintext
 * broker is not forced to authenticate against a broker that would not understand it.
 */
@Configuration(proxyBeanMethods = false)
@Profile("secure-kafka")
public class KafkaSecurityConfiguration {

    private final Map<String, Object> saslProfile;

    KafkaSecurityConfiguration(
            @Value("${spring.application.name}") String applicationName,
            @Value("${fes.kafka.sasl-secret}") String secret,
            @Value("${fes.kafka.security-protocol:SASL_PLAINTEXT}") String securityProtocol) {
        this.saslProfile = saslProfile(applicationName, secret, securityProtocol);
    }

    /** Exposed so a test can state exactly what a service is expected to present. */
    public static Map<String, Object> saslProfile(String applicationName,
                                                    String secret,
                                                    String securityProtocol) {
        return Map.of(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol,
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"%s\" password=\"%s\";".formatted(applicationName, secret));
    }

    @Bean
    DefaultKafkaProducerFactoryCustomizer securityProducerCustomizer() {
        return factory -> factory.updateConfigs(saslProfile);
    }

    @Bean
    DefaultKafkaConsumerFactoryCustomizer securityConsumerCustomizer() {
        return factory -> factory.updateConfigs(saslProfile);
    }
}
