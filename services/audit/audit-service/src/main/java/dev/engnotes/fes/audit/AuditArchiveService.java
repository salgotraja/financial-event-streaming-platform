package dev.engnotes.fes.audit;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Classifies a delivered record and hands it to the sink exactly once per broker coordinate.
 *
 * <p>Delivery is at-least-once (ADR-019), so a rebalance or a restart before the offset commit
 * replays records that were already archived. Deduplication is on topic, partition and offset, the
 * canonical audit idempotency key (architecture-v1.2 line 231), which needs no knowledge of any
 * event's payload.
 *
 * <p><strong>The window is in-process and bounded.</strong> It suppresses the duplicate that
 * at-least-once actually produces, redelivery of uncommitted offsets within a running consumer, and
 * it does not survive a restart or catch a replay older than the window. Restart-safe deduplication
 * belongs to the durable sink, whose object key carries the offset range, and that sink is Phase 3.
 * Nothing here should be read as end-to-end exactly-once; the platform is never described that way.
 */
@Service
public class AuditArchiveService {

    private static final Logger log = LoggerFactory.getLogger(AuditArchiveService.class);

    private final AuditEventDecoder decoder;
    private final AuditSink sink;
    private final Set<String> recentlyArchived;

    public AuditArchiveService(AuditEventDecoder decoder, AuditSink sink, AuditProperties properties) {
        this.decoder = decoder;
        this.sink = sink;
        this.recentlyArchived = boundedKeySet(properties.recentRecordWindow());
    }

    public void archive(ConsumerRecord<String, byte[]> record) {
        ArchivedRecord archived = new ArchivedRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                decoder.eventType(record.topic(), record.value()),
                record.value() == null ? new byte[0] : record.value(),
                Instant.ofEpochMilli(record.timestamp()));

        if (recentlyArchived.contains(archived.idempotencyKey())) {
            log.debug("Skipped duplicate delivery {}", archived.idempotencyKey());
            return;
        }
        // Marked only after the sink accepted it. Marking first would make a failed write look like a
        // completed one to the retry that follows, and the record would be dropped rather than
        // quarantined: a silent hole in the evidence trail.
        sink.write(archived);
        recentlyArchived.add(archived.idempotencyKey());
    }

    private static Set<String> boundedKeySet(int window) {
        Map<String, Boolean> lru = new LinkedHashMap<>(window, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > window;
            }
        };
        return Collections.newSetFromMap(Collections.synchronizedMap(lru));
    }
}
