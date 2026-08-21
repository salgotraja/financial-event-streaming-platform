package dev.engnotes.fes.tradeproducer;

import java.time.Clock;
import java.util.random.RandomGenerator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires trade generation, and only when it is switched on.
 *
 * <p>The gate is on the whole configuration rather than on individual beans, so there is one place
 * that decides whether this service generates traffic. With generation off the module is a
 * publisher and nothing else, which is what the default deployment and the publisher tests expect.
 *
 * <p>FR-01.1 requires this service to publish trades. It does not require it to invent them on
 * boot, and a producer that started emitting synthetic executions in every environment it was
 * deployed into would be a surprise rather than a feature.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "fes.trade-producer.generation", name = "enabled",
        havingValue = "true")
public class TradeGenerationConfiguration {

    @Bean
    RandomGenerator tradeRandomGenerator() {
        return RandomGenerator.getDefault();
    }

    @Bean
    Clock tradeClock() {
        return Clock.systemUTC();
    }

    @Bean
    TradeGenerator tradeGenerator(TradeProducerProperties properties,
                                  RandomGenerator random,
                                  Clock clock) {
        return new TradeGenerator(properties.generation(), random, clock);
    }

    @Bean
    TradeGenerationDriver tradeGenerationDriver(TradeGenerator generator,
                                                TradeEventPublisher publisher,
                                                TradeProducerProperties properties) {
        return new TradeGenerationDriver(generator, publisher, properties.generation());
    }
}
