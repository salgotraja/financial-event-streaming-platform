package dev.engnotes.fes.riskalert;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class RiskAlertServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RiskAlertServiceApplication.class, args);
    }
}
