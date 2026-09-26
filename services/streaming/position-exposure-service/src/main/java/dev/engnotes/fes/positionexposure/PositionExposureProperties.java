package dev.engnotes.fes.positionexposure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param topic            the enriched trade topic this service consumes
 * @param outputTopic      the snapshot topic this service publishes
 * @param consumerInstance carried into every DeadLetterEvent
 */
@ConfigurationProperties(prefix = "fes.position-exposure-service")
public record PositionExposureProperties(String topic, String outputTopic, String consumerInstance) {
}
