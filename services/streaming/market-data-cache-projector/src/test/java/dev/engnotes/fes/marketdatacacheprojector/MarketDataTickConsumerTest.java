package dev.engnotes.fes.marketdatacacheprojector;

import java.time.Instant;

import dev.engnotes.fes.events.MarketDataTickEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("MarketDataTickConsumer")
class MarketDataTickConsumerTest {

    @Test
    @DisplayName("should acknowledge the record when metrics recording throws")
    void should_acknowledge_the_record_when_metrics_recording_throws() {
        MarketDataTickEvent tick = MarketDataTickEvent.newBuilder()
                .setTicker("METRICS-DOWN")
                .setBidPrice(100.0)
                .setAskPrice(101.0)
                .setLastTradedPrice(100.5)
                .setVolume(10L)
                .setEventTimestamp(Instant.ofEpochMilli(5_000L))
                .setProducedAt(Instant.ofEpochMilli(5_005L))
                .setCorrelationId("corr-METRICS-DOWN")
                .build();
        ConsumerRecord<String, MarketDataTickEvent> record =
                new ConsumerRecord<>("market-data.ticks", 0, 42L, "METRICS-DOWN", tick);

        MarketStateProjection projection = mock(MarketStateProjection.class);
        when(projection.project(any(), anyLong()))
                .thenReturn(new ProjectionResult(ProjectionOutcome.APPLIED, true, 1));

        MarketCacheMetrics metrics = mock(MarketCacheMetrics.class);
        doThrow(new RuntimeException("metrics backend unavailable"))
                .when(metrics).record(anyString(), anyLong(), any());

        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        MarketDataTickConsumer consumer = new MarketDataTickConsumer(projection, metrics);

        consumer.project(record, acknowledgment);

        verify(acknowledgment).acknowledge();
    }
}
