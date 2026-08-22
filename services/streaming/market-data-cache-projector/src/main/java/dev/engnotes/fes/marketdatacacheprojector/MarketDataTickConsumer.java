package dev.engnotes.fes.marketdatacacheprojector;

import dev.engnotes.fes.events.MarketDataTickEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Projects each tick into Redis and commits the offset once the projection has been applied.
 *
 * <p>A discarded stale tick is a successful outcome, not a failure: the offset commits either way,
 * because the record has been fully accounted for (ADR-032).
 *
 * <p>Failures propagate to the container's error handler, which decides between the two classes.
 * A malformed payload is one quarantined record; a Redis outage pauses the container without
 * committing (ADR-027).
 */
@Component
public class MarketDataTickConsumer {

    private static final Logger log = LoggerFactory.getLogger(MarketDataTickConsumer.class);

    private final MarketStateProjection projection;
    private final MarketCacheMetrics metrics;

    public MarketDataTickConsumer(MarketStateProjection projection, MarketCacheMetrics metrics) {
        this.projection = projection;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${fes.market-data-cache-projector.topic}")
    public void project(ConsumerRecord<String, MarketDataTickEvent> record,
                        Acknowledgment acknowledgment) {

        MarketDataTickEvent tick = record.value();
        ProjectionOutcome outcome = projection.project(tick, record.offset()).outcome();
        metrics.record(tick.getTicker().toString(), tick.getEventTimestamp().toEpochMilli(), outcome);

        // DEBUG rather than INFO: at the platform's target rate an INFO line per record is the
        // dominant cost of the service. The identity stack test raises this logger instead.
        log.debug("Projected tick ticker={} outcome={} partition={} offset={}",
                tick.getTicker(), outcome, record.partition(), record.offset());

        acknowledgment.acknowledge();
    }
}
