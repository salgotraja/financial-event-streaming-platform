package dev.engnotes.fes.corporateactionproducer;

import dev.engnotes.fes.common.kafka.ProducerDurabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@EnableConfigurationProperties(CorporateActionProducerProperties.class)
@Import(ProducerDurabilityConfiguration.class)
public class CorporateActionProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(CorporateActionProducerApplication.class, args);
    }
}
