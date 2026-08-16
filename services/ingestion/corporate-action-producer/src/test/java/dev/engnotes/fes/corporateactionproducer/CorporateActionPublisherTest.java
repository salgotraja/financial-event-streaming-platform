package dev.engnotes.fes.corporateactionproducer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.CorporateActionEvent;
import dev.engnotes.fes.events.CorporateActionType;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("CorporateActionPublisher")
class CorporateActionPublisherTest {

    @Mock
    private KafkaTemplate<String, CorporateActionEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, CorporateActionEvent>> recordCaptor;

    private CorporateActionPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new CorporateActionPublisher(
                kafkaTemplate, new CorporateActionProducerProperties("corporate-actions"));
    }

    @Test
    @DisplayName("should key the record on ticker so revisions for one instrument stay ordered")
    void should_key_the_record_on_ticker_so_revisions_for_one_instrument_stay_ordered() {
        stubSend();

        publisher.publish(CorporateActions.split("CA-1", "RELIANCE"));

        assertThat(captured().key())
                .as("a correction supersedes the action it corrects, which needs one partition")
                .isEqualTo("RELIANCE");
    }

    @Test
    @DisplayName("should publish to the configured topic")
    void should_publish_to_the_configured_topic() {
        stubSend();

        publisher.publish(CorporateActions.dividend("CA-2", "TCS"));

        assertThat(captured().topic()).isEqualTo("corporate-actions");
    }

    @Test
    @DisplayName("should propagate W3C trace context as record headers")
    void should_propagate_w3c_trace_context_as_record_headers() {
        stubSend();

        publisher.publish(CorporateActions.rightsIssue("CA-3", "INFY"));

        assertThat(header("traceparent")).isEqualTo(CorporateActions.TRACEPARENT);
    }

    @Test
    @DisplayName("should always carry the correlation id as a header")
    void should_always_carry_the_correlation_id_as_a_header() {
        stubSend();

        publisher.publish(CorporateActions.earnings("CA-4", "WIPRO"));

        assertThat(header("correlationId")).isEqualTo("corr-1");
    }

    @Test
    @DisplayName("should omit trace headers when the event carries no trace context")
    void should_omit_trace_headers_when_the_event_carries_no_trace_context() {
        stubSend();
        CorporateActionEvent untraced = CorporateActionEvent.newBuilder(
                        CorporateActions.split("CA-5", "RELIANCE"))
                .setTraceContext(Map.of())
                .build();

        publisher.publish(untraced);

        assertThat(captured().headers().lastHeader("traceparent")).isNull();
        assertThat(header("correlationId")).isEqualTo("corr-1");
    }

    @Test
    @DisplayName("should reject an invalid action before touching the broker")
    void should_reject_an_invalid_action_before_touching_the_broker() {
        CorporateActionEvent noRatio = CorporateActions.action(
                "CA-6", "RELIANCE", CorporateActionType.STOCK_SPLIT, Map.of());

        assertThatThrownBy(() -> publisher.publish(noRatio))
                .isInstanceOf(IllegalArgumentException.class);
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("should surface a delivery failure to the caller rather than swallowing it")
    void should_surface_a_delivery_failure_to_the_caller_rather_than_swallowing_it() {
        CompletableFuture<SendResult<String, CorporateActionEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        CompletableFuture<SendResult<String, CorporateActionEvent>> result =
                publisher.publish(CorporateActions.split("CA-7", "TCS"));

        assertThat(result).isCompletedExceptionally();
    }

    private void stubSend() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private ProducerRecord<String, CorporateActionEvent> captured() {
        verify(kafkaTemplate).send(recordCaptor.capture());
        return recordCaptor.getValue();
    }

    private String header(String name) {
        var header = captured().headers().lastHeader(name);
        assertThat(header).as("header %s is present", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
