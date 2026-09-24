package dev.engnotes.fes.positionexposure;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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

        List<String> callOrder = new ArrayList<>();
        doAnswer(invocation -> {
            callOrder.add("publish");
            return null;
        }).when(publisher).publish(record, position());
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
        verify(publisher, times(1)).publish(record, position());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void a_null_valued_record_is_rejected_without_calling_the_store() {
        ConsumerRecord<String, EnrichedTradeEvent> nullValued =
                new ConsumerRecord<>("trades.enriched", 0, 0L, "RELIANCE", null);

        assertThatThrownBy(() -> consumer.consume(nullValued, acknowledgment))
                .isInstanceOf(IllegalArgumentException.class);

        verify(publisher, never()).publish(any(), any());
        verify(acknowledgment, never()).acknowledge();
    }

    @ParameterizedTest
    @ValueSource(strings = {"tradeId", "accountId", "traderId", "ticker"})
    void a_trade_carrying_the_key_separator_is_rejected_before_the_position_moves(String field) {
        EnrichedTradeEvent trade = trade();
        TradeEvent source = trade.getTrade();
        switch (field) {
            case "tradeId" -> source.setTradeId("t\u001F1");
            case "accountId" -> source.setAccountId("acc\u001F1");
            case "traderId" -> source.setTraderId("trader\u001F1");
            case "ticker" -> source.setTicker("REL\u001FIANCE");
            default -> throw new IllegalStateException(field);
        }
        ConsumerRecord<String, EnrichedTradeEvent> record =
                new ConsumerRecord<>("trades.enriched", 0, 0L, "RELIANCE", trade);

        // The snapshot key and snapshotId would reject this component after the apply had
        // committed, leaving a moved position with no snapshot and a dead letter whose replay
        // fails the same way. Rejecting it first keeps the position untouched.
        assertThatThrownBy(() -> consumer.consume(record, acknowledgment))
                .isInstanceOf(IllegalArgumentException.class);

        verify(store, never()).apply(any());
        verify(publisher, never()).publish(any(), any());
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void the_listener_id_does_not_override_the_configured_consumer_group() throws Exception {
        Method consume = EnrichedTradeConsumer.class.getMethod(
                "consume", ConsumerRecord.class, Acknowledgment.class);
        KafkaListener listener = consume.getAnnotation(KafkaListener.class);

        // Spring Kafka's idIsGroup defaults to true, and an explicit id silently becomes the group.
        // trade-enrichment-service shipped exactly that bug and every functional test passed.
        assertThat(listener.idIsGroup()).isFalse();
        assertThat(listener.id()).isEqualTo(EnrichedTradeConsumer.LISTENER_ID);
    }
}
