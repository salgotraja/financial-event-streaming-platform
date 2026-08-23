package dev.engnotes.fes.tradeenrichment;

import dev.engnotes.fes.common.kafka.ConsumerAcknowledgementConfiguration;
import dev.engnotes.fes.common.kafka.KafkaSecurityConfiguration;
import dev.engnotes.fes.common.kafka.ProducerDurabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@EnableConfigurationProperties(EnrichmentProperties.class)
@Import({ConsumerAcknowledgementConfiguration.class, KafkaSecurityConfiguration.class,
        ProducerDurabilityConfiguration.class})
public class TradeEnrichmentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradeEnrichmentServiceApplication.class, args);
    }
}
