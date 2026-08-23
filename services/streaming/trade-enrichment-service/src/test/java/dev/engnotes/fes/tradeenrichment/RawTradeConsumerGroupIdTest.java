package dev.engnotes.fes.tradeenrichment;

import java.util.HashMap;
import java.util.Map;

import dev.engnotes.fes.events.TradeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.MapPropertySource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A direct, broker-free regression guard for the {@code idIsGroup} defect fixed in {@link
 * RawTradeConsumer}.
 *
 * <p>{@code @KafkaListener(id = "trades-raw", ...)} defaults {@code idIsGroup} to {@code true},
 * which silently overrides the configured {@code group.id} with the listener's own {@code id}.
 * Every other integration test in this module passed regardless of that bug, because none of them
 * runs against a broker that enforces ACLs, so a consumer joining the wrong group reads the topic
 * exactly as well as one joining the right group would. Only {@code TradeEnrichmentServiceIdentityStackTest},
 * which runs a real strict-security broker, actually failed on it, and even that proof is indirect:
 * the granted run only succeeds because the committed policy's {@code GROUP} ACL happens to name the
 * correct group and not {@code trades-raw}. That test is also the slowest one this module has.
 *
 * <p>This test targets the mechanism directly instead. It builds the real
 * {@code ConcurrentKafkaListenerContainerFactory} and registers the real {@link RawTradeConsumer}
 * bean exactly as the production context would, through {@code @EnableKafka}'s own annotation
 * processing, but never calls {@code start()} on the resulting container, so no broker connection is
 * attempted and no Docker is needed.
 *
 * <p>{@link MessageListenerContainer#getGroupId()} is what makes this direct rather than incidental:
 * disassembling {@code AbstractMessageListenerContainer.getGroupId()} in spring-kafka 4.1.0 shows it
 * returns {@code ContainerProperties.getGroupId()} when the endpoint set an override (exactly what
 * {@code idIsGroup = true} does, using the listener {@code id}), and falls back to the consumer
 * factory's own configured {@code group.id} otherwise. It therefore reports the exact group a real
 * consumer would join, without ever constructing one.
 */
@DisplayName("RawTradeConsumer's effective consumer group")
class RawTradeConsumerGroupIdTest {

    private static final String CONFIGURED_GROUP_ID = "trade-enrichment-service";

    @Test
    @DisplayName("should join trade-enrichment-service, not a group named after the listener id")
    void should_join_trade_enrichment_service_not_a_group_named_after_the_listener_id() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
                    "test", Map.of("fes.trade-enrichment-service.topic", "trades.raw")));
            context.register(TestConfig.class);
            context.refresh();

            KafkaListenerEndpointRegistry registry = context.getBean(KafkaListenerEndpointRegistry.class);
            MessageListenerContainer container = registry.getListenerContainer(RawTradeConsumer.LISTENER_ID);

            assertThat(container)
                    .as("the listener container must have been registered under its declared id")
                    .isNotNull();
            assertThat(container.getGroupId())
                    .as("the consumer factory is configured with group.id=" + CONFIGURED_GROUP_ID
                            + "; idIsGroup regressing to its default would report \"trades-raw\" "
                            + "instead, the listener's own id")
                    .isEqualTo(CONFIGURED_GROUP_ID);
        }
    }

    @EnableKafka
    @Configuration(proxyBeanMethods = false)
    static class TestConfig {

        @Bean
        static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
            return new PropertySourcesPlaceholderConfigurer();
        }

        @Bean
        ConsumerFactory<String, TradeEvent> consumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, CONFIGURED_GROUP_ID);
            // Deserializer type is irrelevant: this container is never started, so no record is ever
            // actually deserialized. String keeps the factory constructible without an Avro registry.
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, TradeEvent> kafkaListenerContainerFactory(
                ConsumerFactory<String, TradeEvent> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, TradeEvent> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            return factory;
        }

        @Bean
        RawTradeConsumer rawTradeConsumer() {
            return new RawTradeConsumer(mock(TradeEnricher.class), mock(EnrichedTradePublisher.class),
                    mock(EnrichmentMetrics.class));
        }
    }
}
