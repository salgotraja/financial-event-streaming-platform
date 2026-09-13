package dev.engnotes.fes.positionexposure;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.positionexposure.position.Position;
import dev.engnotes.fes.positionexposure.position.PositionStore;
import dev.engnotes.fes.positionexposure.snapshot.PositionSnapshotPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Applies one trade to the position read model, publishes its snapshot, then commits the offset.
 *
 * <p>The order is contractual under at-least-once (ADR-019): acknowledging before the publish would
 * advance the offset past a trade whose snapshot never reached {@code positions.snapshots}, and a
 * crash between the two would lose that snapshot with nothing left to replay it. A metrics failure
 * is not in that class: by the time metrics runs, the snapshot is already on the topic, so letting it
 * propagate would reapply the trade and republish the same snapshot a second time (harmlessly, since
 * both {@code PositionStore.apply} and the snapshot's identity are idempotent, but pointlessly).
 *
 * <p>This service folds no governance topic, so unlike {@code risk-alert-service} there is no
 * readiness gate here: the listener starts as soon as the context is up.
 */
@Component
public class EnrichedTradeConsumer {

    public static final String LISTENER_ID = "trades-enriched";

    private static final Logger log = LoggerFactory.getLogger(EnrichedTradeConsumer.class);

    private final PositionStore store;
    private final PositionSnapshotPublisher publisher;
    private final PositionExposureMetrics metrics;

    public EnrichedTradeConsumer(PositionStore store,
                                 PositionSnapshotPublisher publisher,
                                 PositionExposureMetrics metrics) {
        this.store = store;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    // idIsGroup = false, for the reason spelled out in EnrichedTradeConsumerGroupIdTest.
    @KafkaListener(id = LISTENER_ID,
            idIsGroup = false,
            topics = "${fes.position-exposure-service.topic}")
    public void consume(ConsumerRecord<String, EnrichedTradeEvent> record,
                        Acknowledgment acknowledgment) {

        if (record.value() == null) {
            // A decode failure never reaches here: ErrorHandlingDeserializer carries it on a
            // DeserializationException instead. A null value with no such header is a distinct
            // payload verdict, so it joins the same zero-retry class as the others rather than
            // NPE-ing into PositionStore.apply.
            throw new IllegalArgumentException("Null value on topic=" + record.topic()
                    + ", partition=" + record.partition() + ", offset=" + record.offset());
        }

        EnrichedTradeEvent trade = record.value();
        Position position = store.apply(trade);
        publisher.publish(position, trade);

        try {
            metrics.recordSnapshot();
        } catch (RuntimeException e) {
            log.warn("Metrics recording failed for tradeId={} partition={} offset={}",
                    trade.getTrade().getTradeId(), record.partition(), record.offset(), e);
        }

        // DEBUG rather than INFO: at the platform's target rate an INFO line per record is the
        // dominant cost of the service.
        log.debug("Applied trade tradeId={} ticker={} partition={} offset={}",
                trade.getTrade().getTradeId(), record.key(), record.partition(), record.offset());

        acknowledgment.acknowledge();
    }
}
