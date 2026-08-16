package dev.engnotes.fes.referencedata;

import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
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
@DisplayName("InstrumentReferencePublisher")
class InstrumentReferencePublisherTest {

    @Mock
    private KafkaTemplate<String, InstrumentReferenceEvent> kafkaTemplate;

    @Mock
    private Propagator propagator;

    @Mock(answer = org.mockito.Answers.RETURNS_DEEP_STUBS)
    private Tracer tracer;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, InstrumentReferenceEvent>> recordCaptor;

    private InstrumentReferencePublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new InstrumentReferencePublisher(kafkaTemplate, propagator, tracer,
                new ReferenceDataProperties("reference-data.instruments", "reference-data-service", null));
    }

    @Test
    @DisplayName("should key the record on instrument id because the topic is compacted")
    void should_key_the_record_on_instrument_id_because_the_topic_is_compacted() {
        stubSend();

        publisher.publish(Instruments.instrument("INS-RELIANCE", "RELIANCE", 1L));

        assertThat(captured().key())
                .as("compaction keeps the latest record per key, so the key is the row identity")
                .isEqualTo("INS-RELIANCE");
    }

    @Test
    @DisplayName("should publish to the configured topic")
    void should_publish_to_the_configured_topic() {
        stubSend();

        publisher.publish(Instruments.instrument("INS-TCS", "TCS", 1L));

        assertThat(captured().topic()).isEqualTo("reference-data.instruments");
    }

    @Test
    @DisplayName("should never publish a null value that would tombstone the instrument")
    void should_never_publish_a_null_value_that_would_tombstone_the_instrument() {
        stubSend();

        publisher.publish(Instruments.instrument("INS-INFY", "INFY", 1L));

        assertThat(captured().value()).isNotNull();
    }

    @Test
    @DisplayName("should overwrite the caller supplied producer identity with the configured one")
    void should_overwrite_the_caller_supplied_producer_identity_with_the_configured_one() {
        stubSend();

        publisher.publish(Instruments.instrument("INS-WIPRO", "WIPRO", 1L, "some-other-workload"));

        assertThat(captured().value().getProducerIdentity())
                .as("FR-10.5 provenance is a stamped fact, not a caller's claim")
                .isEqualTo("reference-data-service");
    }

    @Test
    @DisplayName("should accept an increasing reference version for the same instrument")
    void should_accept_an_increasing_reference_version_for_the_same_instrument() {
        stubSend();

        publisher.publish(Instruments.instrument("INS-RELIANCE", "RELIANCE", 1L));
        publisher.publish(Instruments.instrument("INS-RELIANCE", "RELIANCE", 2L));

        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("should reject a reference version that does not advance")
    void should_reject_a_reference_version_that_does_not_advance() {
        stubSend();
        publisher.publish(Instruments.instrument("INS-RELIANCE", "RELIANCE", 5L));

        assertThatThrownBy(() ->
                publisher.publish(Instruments.instrument("INS-RELIANCE", "RELIANCE", 4L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not advance");
    }

    @Test
    @DisplayName("should reject a repeat of the version already published")
    void should_reject_a_repeat_of_the_version_already_published() {
        stubSend();
        publisher.publish(Instruments.instrument("INS-TCS", "TCS", 3L));

        assertThatThrownBy(() -> publisher.publish(Instruments.instrument("INS-TCS", "TCS", 3L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("should track versions per instrument rather than globally")
    void should_track_versions_per_instrument_rather_than_globally() {
        stubSend();
        publisher.publish(Instruments.instrument("INS-RELIANCE", "RELIANCE", 9L));

        // A low version on a different instrument is the first version of that instrument, not a
        // regression. Tracking one global high-water mark would reject it.
        publisher.publish(Instruments.instrument("INS-INFY", "INFY", 1L));

        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("should reject a non-positive reference version before touching the broker")
    void should_reject_a_non_positive_reference_version_before_touching_the_broker() {
        assertThatThrownBy(() -> publisher.publish(Instruments.instrument("INS-TCS", "TCS", 0L)))
                .isInstanceOf(IllegalArgumentException.class);
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("should omit trace headers when no span is active")
    void should_omit_trace_headers_when_no_span_is_active() {
        stubSend();
        when(tracer.currentTraceContext().context()).thenReturn(null);

        publisher.publish(Instruments.instrument("INS-WIPRO", "WIPRO", 1L));

        assertThat(captured().headers().toArray()).isEmpty();
    }

    @Test
    @DisplayName("should surface a delivery failure to the caller rather than swallowing it")
    void should_surface_a_delivery_failure_to_the_caller_rather_than_swallowing_it() {
        CompletableFuture<SendResult<String, InstrumentReferenceEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        CompletableFuture<SendResult<String, InstrumentReferenceEvent>> result =
                publisher.publish(Instruments.instrument("INS-RELIANCE", "RELIANCE", 1L));

        assertThat(result).isCompletedExceptionally();
    }

    private void stubSend() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private ProducerRecord<String, InstrumentReferenceEvent> captured() {
        verify(kafkaTemplate).send(recordCaptor.capture());
        return recordCaptor.getValue();
    }
}
