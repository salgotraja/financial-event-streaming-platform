package dev.engnotes.fes.corporateactionproducer;

import dev.engnotes.fes.events.CorporateActionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@DisplayName("corporate action seed configuration")
class CorporateActionSeedConfigurationTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

    @Test
    @DisplayName("should not wire the seeder when the seed is not configured")
    void should_not_wire_the_seeder_when_the_seed_is_not_configured() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(CorporateActionSeeder.class));
    }

    @Test
    @DisplayName("should wire the seeder when the seed is switched on")
    void should_wire_the_seeder_when_the_seed_is_switched_on() {
        contextRunner.withPropertyValues("fes.corporate-action-producer.seed.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(CorporateActionSeeder.class));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(CorporateActionProducerProperties.class)
    @Import(CorporateActionSeedConfiguration.class)
    static class TestConfiguration {

        @Bean
        KafkaTemplate<String, CorporateActionEvent> kafkaTemplate() {
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, CorporateActionEvent> template = mock(KafkaTemplate.class);
            return template;
        }

        @Bean
        CorporateActionPublisher corporateActionPublisher(
                KafkaTemplate<String, CorporateActionEvent> kafkaTemplate,
                CorporateActionProducerProperties properties) {
            return new CorporateActionPublisher(kafkaTemplate, properties);
        }
    }
}
