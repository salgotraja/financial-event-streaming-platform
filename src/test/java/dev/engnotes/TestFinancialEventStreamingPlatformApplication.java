package dev.engnotes;

import org.springframework.boot.SpringApplication;

public class TestFinancialEventStreamingPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.from(FinancialEventStreamingPlatformApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
