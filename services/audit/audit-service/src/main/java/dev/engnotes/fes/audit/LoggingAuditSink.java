package dev.engnotes.fes.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The stand-in sink: it logs that a record reached the archive boundary and discards it.
 *
 * <p><strong>This is not evidence storage.</strong> Nothing written here is durable, immutable,
 * digested or signed, so no FR-05 or FR-16 claim rests on it. It exists to keep the consumer path
 * exercisable until the S3 writer lands in Phase 3, and it is deliberately loud about what it is.
 */
@Component
public class LoggingAuditSink implements AuditSink {

    private static final Logger log = LoggerFactory.getLogger(LoggingAuditSink.class);

    @Override
    public void write(ArchivedRecord record) {
        log.info("Archive boundary (not durable storage) topic={} partition={} offset={} eventType={} bytes={}",
                record.topic(), record.partition(), record.offset(), record.eventType(),
                record.payload().length);
    }
}
