package dev.engnotes.fes.common.kafka;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RetryListener;

/**
 * Remembers when a record first failed and how many times it has been attempted, so the
 * {@code DeadLetterEvent} can carry a real {@code retryCount} and {@code firstFailureAt} rather than
 * two values invented at quarantine time.
 *
 * <p>Spring Kafka calls {@link #failedDelivery} once per attempt and {@link #recovered} once the
 * record is handed to the recoverer, so the entry lives exactly as long as the retry sequence. A
 * record that never fails never enters the map.
 *
 * <p>The map is unbounded by design and bounded in practice: an entry exists only between a
 * record's first failure and its recovery, and the error handler always recovers or the container
 * stops. This is per-process state, so a restart mid-retry restarts the count. That is visible in
 * the quarantined event rather than hidden: {@code retryCount} is the attempts this process made.
 */
public class FailureTracker implements RetryListener {

    private record Attempt(Instant firstFailureAt, int attempts) {
    }

    private final Map<String, Attempt> inFlight = new ConcurrentHashMap<>();
    private final Clock clock;

    public FailureTracker() {
        this(Clock.systemUTC());
    }

    public FailureTracker(Clock clock) {
        this.clock = clock;
    }

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception exception, int deliveryAttempt) {
        inFlight.compute(key(record), (_, existing) -> existing == null
                ? new Attempt(clock.instant(), 1)
                : new Attempt(existing.firstFailureAt(), existing.attempts() + 1));
    }

    @Override
    public void recovered(ConsumerRecord<?, ?> record, Exception exception) {
        inFlight.remove(key(record));
    }

    @Override
    public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
        inFlight.remove(key(record));
    }

    /** Attempts made against this record, at least one for a record that reached the recoverer. */
    public int retryCount(ConsumerRecord<?, ?> record) {
        Attempt attempt = inFlight.get(key(record));
        return attempt == null ? 1 : attempt.attempts();
    }

    /** When this record first failed, falling back to now for a record with no recorded attempt. */
    public Instant firstFailureAt(ConsumerRecord<?, ?> record) {
        Attempt attempt = inFlight.get(key(record));
        return attempt == null ? clock.instant() : attempt.firstFailureAt();
    }

    private static String key(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }
}
