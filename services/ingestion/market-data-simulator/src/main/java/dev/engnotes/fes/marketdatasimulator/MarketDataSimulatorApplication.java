package dev.engnotes.fes.marketdatasimulator;

import dev.engnotes.fes.common.kafka.KafkaSecurityConfiguration;
import dev.engnotes.fes.common.kafka.ProducerDurabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@EnableConfigurationProperties(MarketDataSimulatorProperties.class)
@Import({KafkaSecurityConfiguration.class, ProducerDurabilityConfiguration.class})
public class MarketDataSimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MarketDataSimulatorApplication.class, args);
    }
}
