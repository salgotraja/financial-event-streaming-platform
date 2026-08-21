package dev.engnotes.fes.common.kafka;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Kafka security configuration")
class KafkaSecurityConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
            .withUserConfiguration(KafkaSecurityConfiguration.class)
            .withPropertyValues(
                    "spring.application.name=trade-producer",
                    "fes.kafka.sasl-secret=trade-producer-local-secret");

    @Test
    @DisplayName("should derive the SASL username from the application name")
    void should_derive_the_sasl_username_from_the_application_name() {
        runner.withPropertyValues("spring.profiles.active=secure-kafka").run(context -> {
            Map<String, Object> config = producerConfig(context);

            assertThat(config.get(SaslConfigs.SASL_JAAS_CONFIG).toString())
                    .as("a service that could name its own identity could name the administrator's")
                    .contains("username=\"trade-producer\"")
                    .contains("password=\"trade-producer-local-secret\"");
            assertThat(config).containsEntry(SaslConfigs.SASL_MECHANISM, "PLAIN");
            assertThat(config)
                    .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        });
    }

    @Test
    @DisplayName("should override a JAAS config a service tried to set for itself")
    void should_override_a_jaas_config_a_service_tried_to_set_for_itself() {
        runner.withPropertyValues(
                        "spring.profiles.active=secure-kafka",
                        "spring.kafka.properties.sasl.jaas.config="
                                + "org.apache.kafka.common.security.plain.PlainLoginModule "
                                + "required username=\"admin\" password=\"admin-local-secret\";")
                .run(context -> assertThat(producerConfig(context)
                        .get(SaslConfigs.SASL_JAAS_CONFIG).toString())
                        .as("the platform's derivation must win, or the derivation is advice")
                        .contains("username=\"trade-producer\"")
                        .doesNotContain("admin"));
    }

    @Test
    @DisplayName("should apply to consumers as well as producers")
    void should_apply_to_consumers_as_well_as_producers() {
        runner.withPropertyValues("spring.profiles.active=secure-kafka").run(context -> {
            var factory = context.getBean(org.springframework.kafka.core.DefaultKafkaConsumerFactory.class);
            assertThat(factory.getConfigurationProperties().get(SaslConfigs.SASL_JAAS_CONFIG)
                    .toString())
                    .as("audit-service reads, so a producer-only rule would leave it unbound")
                    .contains("username=\"trade-producer\"");
        });
    }

    @Test
    @DisplayName("should stay inactive when the secure profile is not on")
    void should_stay_inactive_when_the_secure_profile_is_not_on() {
        runner.run(context -> assertThat(producerConfig(context))
                .as("a developer stack with no SASL broker must not be forced to authenticate")
                .doesNotContainKey(SaslConfigs.SASL_JAAS_CONFIG));
    }

    private static Map<String, Object> producerConfig(
            org.springframework.context.ApplicationContext context) {
        return context.getBean(DefaultKafkaProducerFactory.class).getConfigurationProperties();
    }
}
