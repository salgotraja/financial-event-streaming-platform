package dev.engnotes.fes.marketdatacacheprojector;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fes.market-data-cache-projector")
public record ProjectorProperties(String consumerInstance) {
}
