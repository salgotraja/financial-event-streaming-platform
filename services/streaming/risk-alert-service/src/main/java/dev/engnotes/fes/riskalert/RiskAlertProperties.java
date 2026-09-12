package dev.engnotes.fes.riskalert;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic               the enriched trade topic, overridable so integration tests do not share
 *                            a topic with another module's tests
 * @param ruleTopic           the governed rule lifecycle topic, not compacted (ADR-035)
 * @param outputTopic         the alert topic
 * @param consumerInstance    carried into every DeadLetterEvent
 * @param ruleTimelineTimeout how long startup waits for the initial fold before failing
 * @param volumeWindowSeconds the rolling volume distribution's horizon, in seconds
 * @param recentTradeHorizonSeconds the self-cross candidate window's horizon, in seconds
 * @param recentTradeCandidateCap the maximum prior trades returned as self-cross candidates
 */
@ConfigurationProperties(prefix = "fes.risk-alert-service")
public record RiskAlertProperties(String topic,
                                  String ruleTopic,
                                  String outputTopic,
                                  String consumerInstance,
                                  Duration ruleTimelineTimeout,
                                  @DefaultValue("3600") long volumeWindowSeconds,
                                  @DefaultValue("3600") long recentTradeHorizonSeconds,
                                  @DefaultValue("200") int recentTradeCandidateCap) {
}
