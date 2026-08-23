package dev.engnotes.fes.tradeenrichment;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param topic                  the raw trade topic, overridable so integration tests do not share
 *                               a topic with another module's tests
 * @param referenceTopic         the compacted instrument master
 * @param outputTopic            the enriched trade topic
 * @param consumerInstance       carried into every DeadLetterEvent
 * @param marketDataMaxAge       the upper bound of the freshness policy (ADR-034)
 * @param instrumentCacheTimeout how long startup waits for the initial fold before failing
 */
@ConfigurationProperties(prefix = "fes.trade-enrichment-service")
public record EnrichmentProperties(String topic,
                                   String referenceTopic,
                                   String outputTopic,
                                   String consumerInstance,
                                   Duration marketDataMaxAge,
                                   Duration instrumentCacheTimeout) {
}
