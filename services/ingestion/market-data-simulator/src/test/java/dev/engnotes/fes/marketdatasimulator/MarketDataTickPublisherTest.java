package dev.engnotes.fes.marketdatasimulator;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.MarketDataTickEvent;
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
@DisplayName("MarketDataTickPublisher")
class MarketDataTickPublisherTest {

    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Mock
    private KafkaTemplate<String, MarketDataTickEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, MarketDataTickEvent>> recordCaptor;

    private MarketDataTickPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new MarketDataTickPublisher(
                kafkaTemplate, new MarketDataSimulatorProperties("market-data.ticks", null, null));
    }

    @Test
    @DisplayName("should key the record on ticker so the cache projection stays ordered per ticker")
    void should_key_the_record_on_ticker_so_the_cache_projection_stays_ordered_per_ticker() {
        stubSend();

        publisher.publish(tick("RELIANCE"));

        assertThat(captured().key())
                .as("last-write-wins into Redis is only correct if ticks for a ticker are ordered")
                .isEqualTo("RELIANCE");
    }

    @Test
    @DisplayName("should publish to the configured topic")
    void should_publish_to_the_configured_topic() {
        stubSend();

        publisher.publish(tick("TCS"));

        assertThat(captured().topic()).isEqualTo("market-data.ticks");
    }

    @Test
    @DisplayName("should propagate W3C trace context as record headers")
    void should_propagate_w3c_trace_context_as_record_headers() {
        stubSend();

        publisher.publish(tick("INFY"));

        assertThat(header("traceparent")).isEqualTo(TRACEPARENT);
    }

    @Test
    @DisplayName("should always carry the correlation id as a header")
    void should_always_carry_the_correlation_id_as_a_header() {
        stubSend();

        publisher.publish(tick("INFY"));

        assertThat(header("correlationId")).isEqualTo("corr-1");
    }

    @Test
    @DisplayName("should omit trace headers when the event carries no trace context")
    void should_omit_trace_headers_when_the_event_carries_no_trace_context() {
        stubSend();
        MarketDataTickEvent untraced = MarketDataTickEvent.newBuilder(tick("WIPRO"))
                .setTraceContext(Map.of())
                .build();

        publisher.publish(untraced);

        assertThat(captured().headers().lastHeader("traceparent")).isNull();
        assertThat(header("correlationId")).isEqualTo("corr-1");
    }

    @Test
    @DisplayName("should surface a delivery failure to the caller rather than swallowing it")
    void should_surface_a_delivery_failure_to_the_caller_rather_than_swallowing_it() {
        CompletableFuture<SendResult<String, MarketDataTickEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        CompletableFuture<SendResult<String, MarketDataTickEvent>> result =
                publisher.publish(tick("RELIANCE"));

        assertThat(result).isCompletedExceptionally();
    }

    private void stubSend() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private ProducerRecord<String, MarketDataTickEvent> captured() {
        org.mockito.Mockito.verify(kafkaTemplate).send(recordCaptor.capture());
        return recordCaptor.getValue();
    }

    private String header(String name) {
        var header = captured().headers().lastHeader(name);
        assertThat(header).as("header %s is present", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static MarketDataTickEvent tick(String ticker) {
        return MarketDataTickEvent.newBuilder()
                .setTicker(ticker)
                .setBidPrice(2_499.50d)
                .setAskPrice(2_500.50d)
                .setLastTradedPrice(2_500.00d)
                .setVolume(12_500L)
                .setEventTimestamp(Instant.parse("2026-08-16T09:15:00Z"))
                .setProducedAt(Instant.parse("2026-08-16T09:15:00.004Z"))
                .setCorrelationId("corr-1")
                .setTraceContext(Map.of("traceparent", TRACEPARENT))
                .build();
    }
}
