package dev.engnotes.fes.marketdatasimulator;

import java.time.Clock;
import java.util.random.RandomGenerator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires tick generation, and only when load simulation mode is switched on (FR-01.4).
 *
 * <p>The gate is on the whole configuration rather than on individual beans, so there is one place
 * that decides whether this service generates traffic. With generation off the module is a publisher
 * and nothing else, which is what the default deployment and the publisher tests expect.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "fes.market-data-simulator.generation", name = "enabled",
        havingValue = "true")
public class TickGenerationConfiguration {

    /**
     * Injected rather than called statically so a test can seed the walk and assert exact values.
     * ADR-006 states no reproducibility requirement, but an unseeded model is one that cannot be
     * tested for the properties the ADR claims for it.
     */
    @Bean
    RandomGenerator marketRandomGenerator() {
        return RandomGenerator.getDefault();
    }

    @Bean
    Clock marketClock() {
        return Clock.systemUTC();
    }

    @Bean
    TickGenerator tickGenerator(MarketDataSimulatorProperties properties,
                                RandomGenerator random,
                                Clock clock) {
        return new TickGenerator(properties.model(), random, clock);
    }

    @Bean
    TickGenerationDriver tickGenerationDriver(TickGenerator generator,
                                              MarketDataTickPublisher publisher,
                                              MarketDataSimulatorProperties properties) {
        return new TickGenerationDriver(generator, publisher, properties.generation());
    }
}
