package dev.engnotes.fes.positionexposure;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.positionexposure.position.Position;
import dev.engnotes.fes.positionexposure.snapshot.PositionSnapshotPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PositionSnapshotPublisherTest {

    @Mock
    private KafkaTemplate<String, PositionSnapshotEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, PositionSnapshotEvent>> recordCaptor;

    private PositionSnapshotPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new PositionSnapshotPublisher(kafkaTemplate, "positions.snapshots");
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private static EnrichedTradeEvent trade() {
        TradeEvent tradeEvent = TradeEvent.newBuilder()
                .setTradeId("t-1")
                .setCorrelationId("corr-1")
                .setTicker("RELIANCE")
                .setQuantity(60L)
                .setPrice(2500.0)
                .setSide(Side.BUY)
                .setTraderId("trader-1")
                .setAccountId("acc-1")
                .setEventTimestamp(Instant.ofEpochMilli(2_000L))
                .setProducedAt(Instant.ofEpochMilli(2_001L))
                .setTraceContext(Map.of())
                .build();

        return EnrichedTradeEvent.newBuilder()
                .setTrade(tradeEvent)
                .setMidPriceAtExecution(2500.0)
                .setSpreadAtExecution(0.5)
                .setVwap5Min(2500.0)
                .setMarketCap(1_700_000.0)
                .setPriceDeviation(0.0)
                .setEnrichedAt(Instant.ofEpochMilli(2_002L))
                .setEnrichmentLatencyMs(1L)
                .setMarketDataAgeMs(50L)
                .build();
    }

    private static Position position() {
        return new Position("acc-1", "trader-1", "RELIANCE", 60L, 60L, 0L,
                new BigDecimal("150000.0000"));
    }

    private ProducerRecord<String, PositionSnapshotEvent> captured() {
        org.mockito.Mockito.verify(kafkaTemplate).send(recordCaptor.capture());
        return recordCaptor.getValue();
    }

    @Test
    void the_snapshot_carries_the_position_and_the_trade_that_produced_it() {
        publisher.publish(position(), trade());

        PositionSnapshotEvent published = captured().value();

        // asOf is the trade's event time, never the wall clock, so a replayed trade republishes an
        // identical snapshot.
        assertThat(published.getAsOf()).isEqualTo(Instant.ofEpochMilli(2_000L));
        assertThat(published.getLastProcessedTradeId()).hasToString("t-1");
        assertThat(published.getNetQuantity()).isEqualTo(60L);
        assertThat(published.getMarketValue()).isEqualTo(150_000.0);
    }

    @Test
    void the_record_is_keyed_on_the_composite_position_key() {
        publisher.publish(position(), trade());

        String sentKey = captured().key();

        // Keying on ticker would put two different positions' snapshots on one partition with no
        // ordering between them, and a consumer folding the topic would be at the mercy of
        // interleaving.
        assertThat(sentKey).isEqualTo("acc-1|trader-1|RELIANCE");
    }

    @Test
    void the_snapshot_is_sent_to_the_configured_topic() {
        publisher.publish(position(), trade());

        assertThat(captured().topic()).isEqualTo("positions.snapshots");
    }

    @Test
    void the_snapshot_id_is_derived_so_a_replay_republishes_the_same_identity() {
        publisher.publish(position(), trade());

        PositionSnapshotEvent published = captured().value();

        assertThat(published.getSnapshotId()).hasToString(
                IdempotencyKeys.deterministic("t-1", "acc-1", "trader-1", "RELIANCE").toString());
    }
}
