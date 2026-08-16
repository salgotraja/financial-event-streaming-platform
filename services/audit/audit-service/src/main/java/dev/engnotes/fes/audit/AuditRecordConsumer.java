package dev.engnotes.fes.audit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The archival consumer for the evidence topics.
 *
 * <p>Values arrive as raw bytes rather than as a deserialised Avro object. That is a deliberate
 * choice for this consumer and not a shortcut: the archive's product is the payload that was
 * published, so decoding on the way in and re-encoding on the way out would store a re-encoding and
 * an integrity check would then verify the re-encoding rather than the evidence. Reading bytes also
 * puts a schema failure inside the listener, where it is one quarantined record, instead of in the
 * deserialiser, where it stalls the partition.
 *
 * <p>The offset is committed here, after the sink accepted the record, never by an auto-commit
 * timer. A failure propagates to the container's error handler, which retries with backoff and then
 * quarantines the single record; the offset advances either way and the partition keeps moving
 * (ADR-027).
 */
@Component
public class AuditRecordConsumer {

    private final AuditArchiveService archiveService;

    public AuditRecordConsumer(AuditArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    @KafkaListener(topics = "#{'${fes.audit-service.topics}'.split(',')}")
    public void archive(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        archiveService.archive(record);
        acknowledgment.acknowledge();
    }
}
