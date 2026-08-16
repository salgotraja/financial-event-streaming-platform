package dev.engnotes.fes.audit;

import dev.engnotes.fes.common.kafka.ConsumerAcknowledgementConfiguration;
import dev.engnotes.fes.common.kafka.ProducerDurabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@EnableConfigurationProperties(AuditProperties.class)
@Import({ConsumerAcknowledgementConfiguration.class, ProducerDurabilityConfiguration.class})
public class AuditServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuditServiceApplication.class, args);
    }
}
