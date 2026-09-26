package dev.engnotes.fes.positionexposure.snapshot;

import java.io.Serial;

/**
 * The snapshot send to {@code positions.snapshots} failed: a synchronous throw from the send, or a
 * send that completed exceptionally. The trade itself is not at fault, so the error handler treats
 * this as a dependency outage and pauses the container rather than dead-lettering it (ADR-027).
 */
public class SnapshotPublishException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public SnapshotPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
