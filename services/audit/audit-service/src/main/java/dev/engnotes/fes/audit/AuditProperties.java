package dev.engnotes.fes.audit;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param topics             the evidence topics this service archives. FR-05.1 names every topic
 *                           whose contents are evidence; the configured list is the subset that has
 *                           a producer today, because subscribing to a topic nothing writes yields
 *                           no signal and fails on a broker with auto-create disabled.
 * @param consumerInstance   stamped onto quarantined records so a dead letter names the instance
 *                           that produced it
 * @param recentRecordWindow how many recently archived records to remember for deduplication
 */
@ConfigurationProperties(prefix = "fes.audit-service")
public record AuditProperties(List<String> topics, String consumerInstance, int recentRecordWindow) {

    public AuditProperties {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("audit-service must archive at least one topic");
        }
        if (recentRecordWindow <= 0) {
            throw new IllegalArgumentException(
                    "recentRecordWindow must be positive: a zero window deduplicates nothing");
        }
    }
}
