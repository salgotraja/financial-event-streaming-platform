package dev.engnotes.fes.tradeproducer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("TradeEventPublisher")
class TradeEventPublisherTest {

    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Mock
    private KafkaTemplate<String, TradeEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, TradeEvent>> recordCaptor;

    private TradeEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new TradeEventPublisher(kafkaTemplate, new TradeProducerProperties("trades.raw"));
    }

    @Test
    @DisplayName("should key the record on ticker so a ticker's trades stay on one partition")
    void should_key_the_record_on_ticker_so_a_tickers_trades_stay_on_one_partition() {
        stubSend();

        publisher.publish(trade("TRD-1", "RELIANCE"));

        assertThat(captured().key())
                .as("risk position state is per trader per ticker and assumes single-partition ordering")
                .isEqualTo("RELIANCE");
    }

    @Test
    @DisplayName("should publish to the configured topic")
    void should_publish_to_the_configured_topic() {
        stubSend();

        publisher.publish(trade("TRD-1", "TCS"));

        assertThat(captured().topic()).isEqualTo("trades.raw");
    }

    @Test
    @DisplayName("should propagate W3C trace context as record headers")
    void should_propagate_w3c_trace_context_as_record_headers() {
        stubSend();

        publisher.publish(trade("TRD-1", "INFY"));

        assertThat(header("traceparent")).isEqualTo(TRACEPARENT);
    }

    @Test
    @DisplayName("should always carry the correlation id as a header")
    void should_always_carry_the_correlation_id_as_a_header() {
        stubSend();

        publisher.publish(trade("TRD-1", "INFY"));

        assertThat(header("correlationId")).isEqualTo("corr-1");
    }

    @Test
    @DisplayName("should omit trace headers when the event carries no trace context")
    void should_omit_trace_headers_when_the_event_carries_no_trace_context() {
        stubSend();
        TradeEvent untraced = TradeEvent.newBuilder(trade("TRD-1", "WIPRO"))
                .setTraceContext(Map.of())
                .build();

        publisher.publish(untraced);

        assertThat(captured().headers().lastHeader("traceparent")).isNull();
        assertThat(header("correlationId")).isEqualTo("corr-1");
    }

    @Test
    @DisplayName("should surface a delivery failure to the caller rather than swallowing it")
    void should_surface_a_delivery_failure_to_the_caller_rather_than_swallowing_it() {
        CompletableFuture<SendResult<String, TradeEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        CompletableFuture<SendResult<String, TradeEvent>> result =
                publisher.publish(trade("TRD-1", "RELIANCE"));

        assertThat(result).isCompletedExceptionally();
    }

    private void stubSend() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private ProducerRecord<String, TradeEvent> captured() {
        org.mockito.Mockito.verify(kafkaTemplate).send(recordCaptor.capture());
        return recordCaptor.getValue();
    }

    private String header(String name) {
        var header = captured().headers().lastHeader(name);
        assertThat(header).as("header %s is present", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static TradeEvent trade(String tradeId, String ticker) {
        return TradeEvent.newBuilder()
                .setTradeId(tradeId)
                .setCorrelationId("corr-1")
                .setTicker(ticker)
                .setQuantity(1_000L)
                .setPrice(2_500.00d)
                .setSide(Side.BUY)
                .setTraderId("TRADER-1")
                .setAccountId("ACC-1")
                .setEventTimestamp(Instant.parse("2026-08-16T09:15:00Z"))
                .setProducedAt(Instant.parse("2026-08-16T09:15:00.004Z"))
                .setTraceContext(Map.of("traceparent", TRACEPARENT))
                .build();
    }
}
