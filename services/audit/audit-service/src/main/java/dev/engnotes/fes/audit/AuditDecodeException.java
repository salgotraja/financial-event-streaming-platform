package dev.engnotes.fes.audit;

/**
 * A payload that cannot be decoded against the registry. Permanent by construction: the bytes will
 * not become valid on a retry, so the record is quarantined on the first attempt rather than after
 * a backoff that can only fail three times (ADR-027).
 */
public class AuditDecodeException extends RuntimeException {

    public AuditDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
