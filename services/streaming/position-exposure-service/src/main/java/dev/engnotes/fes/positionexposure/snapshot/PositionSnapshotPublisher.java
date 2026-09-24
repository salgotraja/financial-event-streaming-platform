package dev.engnotes.fes.positionexposure.snapshot;

import java.math.BigDecimal;
import java.util.List;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
import dev.engnotes.fes.positionexposure.position.Position;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publishes one snapshot per applied trade to {@code positions.snapshots}.
 *
 * <p>Keyed per position, on {@code (accountId, traderId, ticker)}, not on ticker: keying on ticker
 * would put two different positions' snapshots on one partition with no ordering between them, and a
 * consumer folding the topic would be at the mercy of interleaving.
 *
 * <p>The key is {@link IdempotencyKeys#deterministic(String...)} over those three components rather
 * than the components themselves, because {@code accountId} and {@code traderId} are RESTRICTED (the
 * field docs in {@code PositionSnapshotEvent.avsc}) and a record key is exactly what Kafka tooling,
 * logs and the dead-letter topic print. The hash is deterministic, so every snapshot of one position
 * still lands on one partition in order. It keeps raw values out of the key; it is not a secret, and
 * the snapshot body still carries both fields for consumers entitled to read them. Building the key
 * also puts the three components through the separator guard, so a component carrying the reserved
 * separator is a payload verdict before anything is sent. {@code snapshotId} is derived separately,
 * from the trade and the same three components.
 *
 * <p>The trace headers are copied from the consumed record, following {@code RiskAlertPublisher} in
 * {@code risk-alert-service}: {@code traceparent}, {@code tracestate} and {@code correlationId} have to
 * survive the hop on the headers, because end-to-end tracing crosses services that may never
 * deserialise the body. A header absent on the consumed record is not invented on the snapshot.
 *
 * <p>A failed send, whether thrown by {@code send} itself (a serializer that cannot find its schema
 * subject) or surfaced by {@code join} (a broker timeout, too few in-sync replicas), is rethrown as
 * {@link SnapshotPublishException}. The trade is already applied by then and is not at fault, so the
 * error handler treats it as a dependency outage rather than a poison record (ADR-027).
 */
public class PositionSnapshotPublisher {

    private static final List<String> PROPAGATED_HEADERS =
            List.of("traceparent", "tracestate", "correlationId");

    private final KafkaTemplate<String, PositionSnapshotEvent> kafkaTemplate;
    private final String topic;

    public PositionSnapshotPublisher(KafkaTemplate<String, PositionSnapshotEvent> kafkaTemplate,
                                     String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(ConsumerRecord<String, EnrichedTradeEvent> source, Position position) {
        EnrichedTradeEvent trade = source.value();
        String tradeId = trade.getTrade().getTradeId().toString();
        String accountId = position.accountId();
        String traderId = position.traderId();
        String ticker = position.ticker();
        String key = IdempotencyKeys.deterministic(accountId, traderId, ticker).toString();

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
        for (String name : PROPAGATED_HEADERS) {
            Header header = source.headers().lastHeader(name);
            if (header != null) {
                record.headers().add(header);
            }
        }
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
