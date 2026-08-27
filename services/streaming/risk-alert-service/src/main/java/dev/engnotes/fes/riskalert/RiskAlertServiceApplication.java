package dev.engnotes.fes.riskalert;

import dev.engnotes.fes.common.kafka.ConsumerAcknowledgementConfiguration;
import dev.engnotes.fes.common.kafka.KafkaSecurityConfiguration;
import dev.engnotes.fes.common.kafka.ProducerDurabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ConfigurationPropertiesScan
@Import({ConsumerAcknowledgementConfiguration.class, KafkaSecurityConfiguration.class,
        ProducerDurabilityConfiguration.class})
public class RiskAlertServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RiskAlertServiceApplication.class, args);
    }
}
