package dev.engnotes.fes.referencedata;

import java.time.Clock;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the startup seed, and only when it is switched on.
 *
 * <p>The gate sits on the configuration rather than on individual beans so there is one place that
 * decides whether this service announces the master on boot. With the seed off the module is a
 * publisher and nothing else.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "fes.reference-data-service.seed", name = "enabled",
        havingValue = "true")
public class InstrumentMasterSeedConfiguration {

    @Bean
    Clock referenceDataClock() {
        return Clock.systemUTC();
    }

    @Bean
    InstrumentMasterSeeder instrumentMasterSeeder(InstrumentReferencePublisher publisher,
                                                  ReferenceDataProperties properties,
                                                  Clock clock) {
        return new InstrumentMasterSeeder(publisher, properties, clock);
    }
}
