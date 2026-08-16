package dev.engnotes.fes.audit;

/**
 * Where an archived record goes.
 *
 * <p>The implementation that satisfies FR-05 writes bounded Parquet batches to S3 under
 * {@code year=/month=/day=/event_type=} partitioning, emits a sidecar manifest with the object
 * digest and offset ranges, signs the manifest digest with the audit KMS key, and relies on Object
 * Lock retention for immutability. None of that exists yet; it is Phase 3 work.
 *
 * <p>The port exists now so the consumer half can be built and tested against a real broker without
 * anything in this module implying that durable, signed, immutable evidence storage is in place. It
 * is not.
 */
public interface AuditSink {

    void write(ArchivedRecord record);
}
