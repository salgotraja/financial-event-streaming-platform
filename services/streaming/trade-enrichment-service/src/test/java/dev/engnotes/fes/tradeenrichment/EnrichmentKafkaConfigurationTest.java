package dev.engnotes.fes.tradeenrichment;

import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("EnrichmentKafkaConfiguration.quarantine")
class EnrichmentKafkaConfigurationTest {

    private final DeadLetterPublisher publisher = mock(DeadLetterPublisher.class);
    private final EnrichmentMetrics metrics = mock(EnrichmentMetrics.class);

    @Test
    @DisplayName("should swallow a metrics failure rather than block the already-published dead letter")
    void should_swallow_a_metrics_failure_rather_than_block_the_already_published_dead_letter() {
        // setAckAfterHandle(true) means a metrics exception surfacing here would prevent the ack and
        // redeliver the record, quarantining it a second time. RawTradeConsumerTest proves the same
        // guard on the success path; this proves it on the recoverer path.
        @SuppressWarnings("unchecked")
        SendResult<String, DeadLetterEvent> sendResult = mock(SendResult.class);
        when(publisher.publish(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));
        doThrow(new IllegalStateException("registry down")).when(metrics).recordQuarantined();

        assertThatCode(() -> EnrichmentKafkaConfiguration.quarantine(
                publisher, metrics, record(), new IllegalArgumentException("bad trade")))
                .doesNotThrowAnyException();

        verify(publisher).publish(any(), any(), any());
    }

    private static ConsumerRecord<String, TradeEvent> record() {
        return new ConsumerRecord<>("trades.raw", 0, 0L, "RELIANCE", trade());
    }

    private static TradeEvent trade() {
        return TradeEvent.newBuilder()
                .setTradeId("T-1").setCorrelationId("C-1").setTicker("RELIANCE")
                .setQuantity(10L).setPrice(105.0).setSide(Side.BUY)
                .setTraderId("TR-1").setAccountId("AC-1")
                .setEventTimestamp(java.time.Instant.ofEpochMilli(1_000L))
                .setProducedAt(java.time.Instant.ofEpochMilli(1_005L))
                .build();
    }
}
