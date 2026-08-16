package dev.engnotes.fes.audit;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("AuditArchiveService")
class AuditArchiveServiceTest {

    private static final String TOPIC = "trades.raw";

    private final RecordingSink sink = new RecordingSink();

    @Test
    @DisplayName("should write once when the same record is delivered twice")
    void should_write_once_when_the_same_record_is_delivered_twice() {
        // At-least-once means redelivery after a rebalance, not a hypothetical (ADR-019).
        AuditArchiveService service = service(1_000);
        ConsumerRecord<String, byte[]> record = record(0, 17L);

        service.archive(record);
        service.archive(record);

        assertThat(sink.written).hasSize(1);
    }

    @Test
    @DisplayName("should treat the same offset on a different partition as a different record")
    void should_treat_the_same_offset_on_a_different_partition_as_a_different_record() {
        AuditArchiveService service = service(1_000);

        service.archive(record(0, 17L));
        service.archive(record(1, 17L));

        assertThat(sink.written).extracting(ArchivedRecord::idempotencyKey)
                .containsExactly("trades.raw-0-17", "trades.raw-1-17");
    }

    @Test
    @DisplayName("should archive again once a record falls out of the bounded window")
    void should_archive_again_once_a_record_falls_out_of_the_bounded_window() {
        // The window is in-process and bounded, so it suppresses redelivery and nothing more.
        // Asserting the limit keeps it from being mistaken for exactly-once.
        AuditArchiveService service = service(1);

        service.archive(record(0, 1L));
        service.archive(record(0, 2L));
        service.archive(record(0, 1L));

        assertThat(sink.written).hasSize(3);
    }

    @Test
    @DisplayName("should archive the delivered bytes and the broker coordinates")
    void should_archive_the_delivered_bytes_and_the_broker_coordinates() {
        AuditArchiveService service = service(1_000);

        service.archive(record(2, 99L));

        ArchivedRecord archived = sink.written.getFirst();
        assertThat(archived.topic()).isEqualTo(TOPIC);
        assertThat(archived.partition()).isEqualTo(2);
        assertThat(archived.offset()).isEqualTo(99L);
        assertThat(archived.key()).isEqualTo("RELIANCE");
        assertThat(archived.eventType()).isEqualTo("TradeEvent");
        assertThat(archived.payload()).isEqualTo(new byte[]{0, 0, 0, 0, 1, 42});
    }

    @Test
    @DisplayName("should archive on retry when the sink rejected the first attempt")
    void should_archive_on_retry_when_the_sink_rejected_the_first_attempt() {
        // Deduplicating on the attempt rather than on the write would make a failed write look
        // completed to the retry behind it, and the record would vanish instead of being archived
        // or quarantined.
        sink.rejectNext();
        AuditArchiveService service = service(1_000);
        ConsumerRecord<String, byte[]> record = record(0, 5L);

        assertThatThrownBy(() -> service.archive(record)).isInstanceOf(IllegalStateException.class);
        service.archive(record);

        assertThat(sink.written).hasSize(1);
    }

    @Test
    @DisplayName("should not write an undecodable payload to the archive")
    void should_not_write_an_undecodable_payload_to_the_archive() {
        // The failure has to reach the container's error handler for the record to be quarantined,
        // so it must propagate rather than be swallowed into a partial write.
        AuditEventDecoder decoder = mock(AuditEventDecoder.class);
        when(decoder.eventType(anyString(), any()))
                .thenThrow(new AuditDecodeException("undecodable", null));
        AuditArchiveService service = new AuditArchiveService(decoder, sink, properties(1_000));

        assertThatThrownBy(() -> service.archive(record(0, 1L)))
                .isInstanceOf(AuditDecodeException.class);
        assertThat(sink.written).isEmpty();
    }

    private AuditArchiveService service(int window) {
        AuditEventDecoder decoder = mock(AuditEventDecoder.class);
        when(decoder.eventType(anyString(), any())).thenReturn("TradeEvent");
        return new AuditArchiveService(decoder, sink, properties(window));
    }

    private static AuditProperties properties(int window) {
        return new AuditProperties(List.of(TOPIC), "audit-service-test", window);
    }

    private static ConsumerRecord<String, byte[]> record(int partition, long offset) {
        return new ConsumerRecord<>(TOPIC, partition, offset, "RELIANCE", new byte[]{0, 0, 0, 0, 1, 42});
    }

    private static final class RecordingSink implements AuditSink {

        private final List<ArchivedRecord> written = new ArrayList<>();
        private boolean rejectNext;

        void rejectNext() {
            rejectNext = true;
        }

        @Override
        public void write(ArchivedRecord record) {
            if (rejectNext) {
                rejectNext = false;
                throw new IllegalStateException("sink unavailable");
            }
            written.add(record);
        }
    }
}
