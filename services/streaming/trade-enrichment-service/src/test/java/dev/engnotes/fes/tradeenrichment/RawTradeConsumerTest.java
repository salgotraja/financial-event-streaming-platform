package dev.engnotes.fes.tradeenrichment;

import java.time.Instant;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("RawTradeConsumer")
class RawTradeConsumerTest {

    private final TradeEnricher enricher = mock(TradeEnricher.class);
    private final EnrichedTradePublisher publisher = mock(EnrichedTradePublisher.class);
    private final EnrichmentMetrics metrics = mock(EnrichmentMetrics.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final RawTradeConsumer consumer = new RawTradeConsumer(enricher, publisher, metrics);

    @Test
    @DisplayName("should acknowledge only after the enriched record has been published")
    void should_acknowledge_only_after_the_enriched_record_has_been_published() {
        // At-least-once (ADR-019): acknowledging before the send would lose the trade entirely if
        // the send failed, because the offset would have advanced past it.
        when(enricher.enrich(any(), anyLong())).thenReturn(enriched());

        consumer.consume(record(), acknowledgment);

        var order = org.mockito.Mockito.inOrder(publisher, acknowledgment);
        order.verify(publisher).publish(any(), any());
        order.verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("should not acknowledge when enrichment fails, so the error handler decides")
    void should_not_acknowledge_when_enrichment_fails_so_the_error_handler_decides() {
        when(enricher.enrich(any(), anyLong())).thenThrow(
                new ReferenceDataUnavailableException(UnavailableReason.STALE, "too old"));

        assertThatThrownBy(() -> consumer.consume(record(), acknowledgment))
                .isInstanceOf(ReferenceDataUnavailableException.class);

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("should swallow a metrics failure rather than requarantine a published record")
    void should_swallow_a_metrics_failure_rather_than_requarantine_a_published_record() {
        // The record is already on trades.enriched by the time metrics runs. Letting a telemetry
        // failure propagate would retry the enrichment and publish the trade a second time.
        when(enricher.enrich(any(), anyLong())).thenReturn(enriched());
        doThrow(new IllegalStateException("registry down")).when(metrics).recordEnriched(any());

        consumer.consume(record(), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    private static ConsumerRecord<String, TradeEvent> record() {
        return new ConsumerRecord<>("trades.raw", 0, 0L, "RELIANCE", trade());
    }

    private static TradeEvent trade() {
        return TradeEvent.newBuilder()
                .setTradeId("T-1").setCorrelationId("C-1").setTicker("RELIANCE")
                .setQuantity(10L).setPrice(105.0).setSide(Side.BUY)
                .setTraderId("TR-1").setAccountId("AC-1")
                .setEventTimestamp(Instant.ofEpochMilli(1_000L))
                .setProducedAt(Instant.ofEpochMilli(1_005L))
                .build();
    }

    private static EnrichedTradeEvent enriched() {
        return EnrichedTradeEvent.newBuilder()
                .setTrade(trade()).setMidPriceAtExecution(100.0).setSpreadAtExecution(2.0)
                .setVwap5Min(100.0).setMarketCap(10.0).setPriceDeviation(5.0)
                .setEnrichedAt(Instant.ofEpochMilli(1_010L)).setEnrichmentLatencyMs(5L)
                .setMarketDataAgeMs(1_000L).build();
    }
}
