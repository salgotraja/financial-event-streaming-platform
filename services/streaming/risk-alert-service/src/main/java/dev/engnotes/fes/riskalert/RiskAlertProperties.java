package dev.engnotes.fes.riskalert;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param topic               the enriched trade topic, overridable so integration tests do not share
 *                            a topic with another module's tests
 * @param ruleTopic           the governed rule lifecycle topic, not compacted (ADR-035)
 * @param outputTopic         the alert topic
 * @param consumerInstance    carried into every DeadLetterEvent
 * @param ruleTimelineTimeout how long startup waits for the initial fold before failing
 */
@ConfigurationProperties(prefix = "fes.risk-alert-service")
public record RiskAlertProperties(String topic,
                                  String ruleTopic,
                                  String outputTopic,
                                  String consumerInstance,
                                  Duration ruleTimelineTimeout) {
}
