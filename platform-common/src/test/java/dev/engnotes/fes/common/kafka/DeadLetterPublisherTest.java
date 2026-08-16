package dev.engnotes.fes.common.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.DeadLetterEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("DeadLetterPublisher")
class DeadLetterPublisherTest {

    private static final Instant FIRST_FAILURE = Instant.parse("2026-08-16T09:15:00Z");
    private static final Instant LAST_FAILURE = Instant.parse("2026-08-16T09:15:04Z");

    @Mock
    private KafkaTemplate<String, DeadLetterEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<String> topicCaptor;

    @Captor
    private ArgumentCaptor<String> keyCaptor;

    @Captor
    private ArgumentCaptor<DeadLetterEvent> eventCaptor;

    private FailureTracker failureTracker;
    private DeadLetterPublisher publisher;

    @BeforeEach
    void setUp() {
        failureTracker = new FailureTracker(Clock.fixed(FIRST_FAILURE, ZoneOffset.UTC));
        publisher = new DeadLetterPublisher(kafkaTemplate, failureTracker, "audit-service",
                "audit-service-7", Clock.fixed(LAST_FAILURE, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("should quarantine to the lowercase .dlq topic of the source topic")
    void should_quarantine_to_the_lowercase_dlq_topic_of_the_source_topic() {
        stubSend();

        publisher.publish(record("trades.raw", new byte[]{1, 2}), new byte[]{1, 2}, failure());

        assertThat(captureTopic())
                .as("replay tooling reads .dlq, not Spring Kafka's default .DLT")
                .isEqualTo("trades.raw.dlq");
    }

    @Test
    @DisplayName("should keep the original key so a replay preserves per-key order")
    void should_keep_the_original_key_so_a_replay_preserves_per_key_order() {
        stubSend();

        publisher.publish(record("trades.raw", new byte[]{1}), new byte[]{1}, failure());

        send();
        assertThat(keyCaptor.getValue()).isEqualTo("RELIANCE");
    }

    @Test
    @DisplayName("should carry the delivered bytes rather than a re-encoding of them")
    void should_carry_the_delivered_bytes_rather_than_a_re_encoding_of_them() {
        stubSend();
        byte[] delivered = {0, 0, 0, 0, 7, 42};

        publisher.publish(record("trades.raw", delivered), delivered, failure());

        assertThat(captureEvent().getOriginalPayload().array()).isEqualTo(delivered);
    }

    @Test
    @DisplayName("should record the broker coordinates that identify the quarantined record")
    void should_record_the_broker_coordinates_that_identify_the_quarantined_record() {
        stubSend();

        publisher.publish(record("trades.raw", new byte[]{1}), new byte[]{1}, failure());

        DeadLetterEvent event = captureEvent();
        assertThat(event.getOriginalTopic()).isEqualTo("trades.raw");
        assertThat(event.getOriginalPartition()).isEqualTo(3);
        assertThat(event.getOriginalOffset()).isEqualTo(4242L);
    }

    @Test
    @DisplayName("should report the attempts actually made and when the record first failed")
    void should_report_the_attempts_actually_made_and_when_the_record_first_failed() {
        stubSend();
        ConsumerRecord<String, byte[]> failed = record("trades.raw", new byte[]{1});
        Exception cause = failure();
        failureTracker.failedDelivery(failed, cause, 1);
        failureTracker.failedDelivery(failed, cause, 2);
        failureTracker.failedDelivery(failed, cause, 3);

        publisher.publish(failed, new byte[]{1}, cause);

        DeadLetterEvent event = captureEvent();
        assertThat(event.getRetryCount()).isEqualTo(3);
        assertThat(event.getFirstFailureAt()).isEqualTo(FIRST_FAILURE);
        assertThat(event.getLastFailureAt()).isEqualTo(LAST_FAILURE);
    }

    @Test
    @DisplayName("should carry the correlation id from the failed record's headers")
    void should_carry_the_correlation_id_from_the_failed_records_headers() {
        stubSend();
        ConsumerRecord<String, byte[]> failed = record("corporate-actions", new byte[]{1});
        failed.headers().add(new RecordHeader("correlationId", "corr-9".getBytes(StandardCharsets.UTF_8)));

        publisher.publish(failed, new byte[]{1}, failure());

        assertThat(captureEvent().getCorrelationId()).isEqualTo("corr-9");
    }

    @Test
    @DisplayName("should leave the correlation id null when the source record carried none")
    void should_leave_the_correlation_id_null_when_the_source_record_carried_none() {
        stubSend();

        publisher.publish(record("market-data.ticks", new byte[]{1}), new byte[]{1}, failure());

        assertThat(captureEvent().getCorrelationId())
                .as("an invented correlation id would correlate nothing")
                .isNull();
    }

    @Test
    @DisplayName("should name the root cause, not the wrapper the container reported")
    void should_name_the_root_cause_not_the_wrapper_the_container_reported() {
        // Spring Kafka hands the recoverer a ListenerExecutionFailedException. Recording that class
        // would make every dead letter on the platform look identical to an operator triaging one.
        stubSend();
        Exception wrapped = new IllegalStateException("sink rejected the batch",
                new java.io.IOException("connection reset"));

        publisher.publish(record("trades.raw", new byte[]{1}), new byte[]{1}, wrapped);

        DeadLetterEvent event = captureEvent();
        assertThat(event.getExceptionClass()).isEqualTo(java.io.IOException.class.getName());
        assertThat(event.getExceptionMessage()).isEqualTo("connection reset");
        assertThat(event.getFailureReason()).contains("sink rejected the batch");
    }

    @Test
    @DisplayName("should quarantine an empty payload rather than fail when the record was a tombstone")
    void should_quarantine_an_empty_payload_rather_than_fail_when_the_record_was_a_tombstone() {
        stubSend();

        publisher.publish(record("reference-data.instruments", null), null, failure());

        assertThat(captureEvent().getOriginalPayload().array()).isEmpty();
    }

    private static ConsumerRecord<String, byte[]> record(String topic, byte[] payload) {
        return new ConsumerRecord<>(topic, 3, 4242L, "RELIANCE", payload);
    }

    private static Exception failure() {
        return new IllegalStateException("sink unavailable");
    }

    private void stubSend() {
        when(kafkaTemplate.send(anyString(), any(), any(DeadLetterEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void send() {
        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), eventCaptor.capture());
    }

    private String captureTopic() {
        send();
        return topicCaptor.getValue();
    }

    private DeadLetterEvent captureEvent() {
        send();
        return eventCaptor.getValue();
    }
}
