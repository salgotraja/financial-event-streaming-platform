package dev.engnotes.fes.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * What a poison record costs before it is quarantined, and which bytes the dead letter carries.
 *
 * <p>Both halves were copied identically into every consumer that quarantines per record (ADR-027).
 * They are shared here because they must not drift: the retry bound decides how long one bad record
 * holds its partition, and a service that quietly widened it would turn a bounded quarantine into
 * unbounded lag on a topic nobody is watching. A service that genuinely needs a different bound
 * states it at its own call site rather than editing these values.
 *
 * <p>This is the bound for a record whose bytes cannot improve. It is not the bound for a failing
 * dependency: a consumer that can distinguish an outage supplies its own back-off for that branch,
 * because retrying a reachable-again dependency is progress while retrying a malformed payload is
 * not.
 */
public final class PoisonRecordPolicy {

    private static final long MAX_RETRIES = 2;
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 5_000;
    private static final long MAX_ELAPSED_MS = 5_000;

    private PoisonRecordPolicy() {
    }

    /**
     * The bytes to preserve in the dead letter for a record that could not be processed.
     *
     * <p>On a decode failure the record value is null, and the only copy of the original bytes is
     * the one {@code ErrorHandlingDeserializer} attached to the {@link DeserializationException}.
     * That exception arrives wrapped in whatever the listener container threw, so the cause chain is
     * walked rather than type-checked at the top. Everything else falls back to the record value,
     * and a value that is not {@code byte[]} yields null: a record that decoded and then failed
     * downstream has no original bytes, and rendering the deserialized object would put a fabricated
     * payload in the evidence trail.
     */
    public static byte[] originalPayload(ConsumerRecord<String, ?> record, Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause();
             cause = cause.getCause()) {
            if (cause instanceof DeserializationException deserialization) {
                return deserialization.getData();
            }
        }
        return record.value() instanceof byte[] bytes ? bytes : null;
    }

    /**
     * Three attempts in total, exponential from 100ms to a 5s cap, capped again at 5s elapsed.
     *
     * <p>A new instance every call. {@link BackOff} is stateful once started, so a shared instance
     * would carry one listener's exhausted attempt count into the next.
     */
    public static BackOff poisonBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(INITIAL_BACKOFF_MS);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        backOff.setMaxElapsedTime(MAX_ELAPSED_MS);
        backOff.setMaxAttempts(MAX_RETRIES);
        return backOff;
    }
}
