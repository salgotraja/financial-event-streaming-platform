package dev.engnotes.fes.positionexposure.snapshot;

import java.math.BigDecimal;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
import dev.engnotes.fes.positionexposure.position.Position;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes one snapshot per applied trade to {@code positions.snapshots}.
 *
 * <p>Keyed on the composite position key, {@code accountId + "|" + traderId + "|" + ticker}, not on
 * ticker: keying on ticker would put two different positions' snapshots on one partition with no
 * ordering between them, and a consumer folding the topic would be at the mercy of interleaving. The
 * three components are platform-controlled identifiers rather than user input, which is what makes a
 * plain {@code '|'} delimiter safe here: it is a printable separator rather than a control character,
 * because this value appears in Kafka tooling, logs and any consumer's own keying, and a 0x1F byte in
 * a record key is hostile to all three. {@code snapshotId} below uses
 * {@link IdempotencyKeys#deterministic(String...)} separately, where the internal separator-rejection
 * guard does apply.
 *
 * <p>A failed send, whether thrown by {@code send} itself (a serializer that cannot find its schema
 * subject) or surfaced by {@code join} (a broker timeout, too few in-sync replicas), is rethrown as
 * {@link SnapshotPublishException}. The trade is already applied by then and is not at fault, so the
 * error handler treats it as a dependency outage rather than a poison record (ADR-027).
 */
@Component
public class PositionSnapshotPublisher {

    private final KafkaTemplate<String, PositionSnapshotEvent> kafkaTemplate;
    private final String topic;

    public PositionSnapshotPublisher(KafkaTemplate<String, PositionSnapshotEvent> kafkaTemplate,
                                     String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(Position position, EnrichedTradeEvent trade) {
        String tradeId = trade.getTrade().getTradeId().toString();
        String accountId = position.accountId();
        String traderId = position.traderId();
        String ticker = position.ticker();
        String key = accountId + "|" + traderId + "|" + ticker;

        PositionSnapshotEvent snapshot = PositionSnapshotEvent.newBuilder()
                .setSnapshotId(IdempotencyKeys.deterministic(tradeId, accountId, traderId, ticker).toString())
                .setAccountId(accountId)
                .setTraderId(traderId)
                .setTicker(ticker)
                .setNetQuantity(position.netQuantity())
                .setGrossBuyQuantity(position.grossBuyQuantity())
                .setGrossSellQuantity(position.grossSellQuantity())
                // The stored NUMERIC(19,4) is the authoritative figure; this double is a lossy
                // rendering of it, which matters when increment 2 reconciles.
                .setMarketValue(toDouble(position.marketValue()))
                // The trade's own eventTimestamp, never the wall clock, so a replayed trade
                // republishes an identical snapshot (ADR-035).
                .setAsOf(trade.getTrade().getEventTimestamp())
                .setLastProcessedTradeId(tradeId)
                .build();

        ProducerRecord<String, PositionSnapshotEvent> record = new ProducerRecord<>(topic, key, snapshot);
        // Only the send is wrapped. Building the snapshot above stays outside, so a separator
        // character rejected by IdempotencyKeys remains an IllegalArgumentException, a verdict on
        // the payload, rather than being mistaken for the topic or registry being unavailable.
        try {
            kafkaTemplate.send(record).join();
        } catch (RuntimeException e) {
            throw new SnapshotPublishException("Snapshot publish to " + topic + " failed for tradeId="
                    + tradeId, e);
        }
    }

    private static double toDouble(BigDecimal marketValue) {
        return marketValue.doubleValue();
    }
}
