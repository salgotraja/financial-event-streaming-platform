package dev.engnotes.fes.tradeproducer;

import dev.engnotes.fes.events.TradeEvent;
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

@DisplayName("trade generation configuration")
class TradeGenerationConfigurationTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

    @Test
    @DisplayName("should not wire the driver when generation is not configured")
    void should_not_wire_the_driver_when_generation_is_not_configured() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(TradeGenerationDriver.class));
    }

    @Test
    @DisplayName("should wire the driver when generation is switched on")
    void should_wire_the_driver_when_generation_is_switched_on() {
        contextRunner.withPropertyValues("fes.trade-producer.generation.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(TradeGenerationDriver.class));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(TradeProducerProperties.class)
    @Import(TradeGenerationConfiguration.class)
    static class TestConfiguration {

        @Bean
        KafkaTemplate<String, TradeEvent> kafkaTemplate() {
            @SuppressWarnings("unchecked")
            KafkaTemplate<String, TradeEvent> template = mock(KafkaTemplate.class);
            return template;
        }

        @Bean
        TradeEventPublisher tradeEventPublisher(KafkaTemplate<String, TradeEvent> kafkaTemplate,
                                                 TradeProducerProperties properties) {
            return new TradeEventPublisher(kafkaTemplate, properties);
        }
    }
}
