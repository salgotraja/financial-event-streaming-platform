package dev.engnotes.fes.corporateactionproducer;

import java.time.Clock;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the startup announcement, and only when it is switched on.
 *
 * <p>The gate sits on the configuration rather than on individual beans so there is one place that
 * decides whether this service announces anything on boot. With the seed off the module is a
 * publisher and nothing else.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "fes.corporate-action-producer.seed", name = "enabled",
        havingValue = "true")
public class CorporateActionSeedConfiguration {

    @Bean
    Clock corporateActionClock() {
        return Clock.systemUTC();
    }

    @Bean
    CorporateActionSeeder corporateActionSeeder(CorporateActionPublisher publisher,
                                                CorporateActionProducerProperties properties,
                                                Clock clock) {
        return new CorporateActionSeeder(publisher, properties, clock);
    }
}
