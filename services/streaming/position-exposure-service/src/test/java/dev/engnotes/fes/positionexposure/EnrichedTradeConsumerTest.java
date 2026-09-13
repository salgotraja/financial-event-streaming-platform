package dev.engnotes.fes.positionexposure;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.positionexposure.position.Position;
import dev.engnotes.fes.positionexposure.position.PositionStore;
import dev.engnotes.fes.positionexposure.snapshot.PositionSnapshotPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class EnrichedTradeConsumerTest {

    private final PositionStore store = mock(PositionStore.class);
    private final PositionSnapshotPublisher publisher = mock(PositionSnapshotPublisher.class);
    private final PositionExposureMetrics metrics = mock(PositionExposureMetrics.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);

    private final EnrichedTradeConsumer consumer =
            new EnrichedTradeConsumer(store, publisher, metrics);

    private static ConsumerRecord<String, EnrichedTradeEvent> record() {
        return new ConsumerRecord<>("trades.enriched", 0, 0L, "RELIANCE", trade());
    }

    private static EnrichedTradeEvent trade() {
        TradeEvent tradeEvent = TradeEvent.newBuilder()
                .setTradeId("t-1")
                .setCorrelationId("corr-1")
                .setTicker("RELIANCE")
                .setQuantity(100L)
                .setPrice(2500.0)
                .setSide(Side.BUY)
                .setTraderId("trader-1")
                .setAccountId("acc-1")
                .setEventTimestamp(Instant.ofEpochMilli(1_000L))
                .setProducedAt(Instant.ofEpochMilli(1_001L))
                .setTraceContext(Map.of())
                .build();

        return EnrichedTradeEvent.newBuilder()
                .setTrade(tradeEvent)
                .setMidPriceAtExecution(2500.0)
                .setSpreadAtExecution(0.5)
                .setVwap5Min(2500.0)
                .setMarketCap(1_700_000.0)
                .setPriceDeviation(0.0)
                .setEnrichedAt(Instant.ofEpochMilli(1_002L))
                .setEnrichmentLatencyMs(1L)
                .setMarketDataAgeMs(50L)
                .build();
    }

    private static Position position() {
        return new Position("acc-1", "trader-1", "RELIANCE", 100L, 100L, 0L,
                new BigDecimal("250000.0000"));
    }

    @Test
    void the_snapshot_is_published_before_the_offset_is_acknowledged() {
        ConsumerRecord<String, EnrichedTradeEvent> record = record();
        when(store.apply(record.value())).thenReturn(position());

        java.util.List<String> callOrder = new java.util.ArrayList<>();
        doAnswer(invocation -> {
            callOrder.add("publish");
            return null;
        }).when(publisher).publish(position(), record.value());
        doAnswer(invocation -> {
            callOrder.add("acknowledge");
            return null;
        }).when(acknowledgment).acknowledge();

        consumer.consume(record, acknowledgment);

        // If this order inverts, a crash between acknowledge and publish loses a snapshot with the
        // offset already committed, and nothing replays it.
        assertThat(callOrder).containsExactly("publish", "acknowledge");
    }

    @Test
    void a_metrics_failure_after_a_successful_publish_does_not_prevent_the_acknowledgement() {
        ConsumerRecord<String, EnrichedTradeEvent> record = record();
        when(store.apply(record.value())).thenReturn(position());
        doThrow(new IllegalStateException("meter registry closed"))
                .when(metrics).recordSnapshot();

        consumer.consume(record, acknowledgment);

        // By the time metrics runs, the snapshot is already on the topic, so letting this propagate
        // would redeliver a trade whose snapshot already went out.
        verify(publisher, times(1)).publish(position(), record.value());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void a_null_valued_record_is_rejected_without_calling_the_store() {
        ConsumerRecord<String, EnrichedTradeEvent> nullValued =
                new ConsumerRecord<>("trades.enriched", 0, 0L, "RELIANCE", null);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> consumer.consume(nullValued, acknowledgment))
                .isInstanceOf(IllegalArgumentException.class);

        verify(publisher, never()).publish(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void the_listener_id_does_not_override_the_configured_consumer_group() throws Exception {
        java.lang.reflect.Method consume = EnrichedTradeConsumer.class.getMethod(
                "consume", ConsumerRecord.class, Acknowledgment.class);
        org.springframework.kafka.annotation.KafkaListener listener =
                consume.getAnnotation(org.springframework.kafka.annotation.KafkaListener.class);

        // Spring Kafka's idIsGroup defaults to true, and an explicit id silently becomes the group.
        // trade-enrichment-service shipped exactly that bug and every functional test passed.
        assertThat(listener.idIsGroup()).isFalse();
        assertThat(listener.id()).isEqualTo(EnrichedTradeConsumer.LISTENER_ID);
    }
}
