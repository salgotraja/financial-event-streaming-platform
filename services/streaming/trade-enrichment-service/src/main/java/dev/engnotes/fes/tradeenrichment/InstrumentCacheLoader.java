package dev.engnotes.fes.tradeenrichment;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Folds the compacted instrument master into {@link InstrumentCache}, and decides when the trade
 * listener may start.
 *
 * <p><strong>assign(), not subscribe().</strong> {@code reference-data.instruments} is compacted
 * across 6 partitions keyed by instrument id, while {@code trades.raw} is 12 partitions keyed by
 * ticker. The two cannot be aligned, so any instance can be asked for any ticker and every instance
 * needs every instrument. A group subscription would hand each instance a subset, which is exactly
 * wrong. Assigning every partition also means no consumer group, so no GROUP grant appears in this
 * service's Kafka policy for the reference topic, and no offsets are committed.
 *
 * <p><strong>The termination condition is positional, and the two obvious alternatives are both
 * wrong on a compacted topic.</strong> Waiting for a record count cannot work because nobody knows
 * the count in advance. Waiting to see a record at {@code endOffset - 1} cannot work because
 * compaction can remove the record at any offset including that one, so the wait would never end.
 * The condition is {@code position(tp) >= capturedEndOffset(tp)} for every partition, against end
 * offsets captured once at startup. A partition whose captured end equals its beginning is already
 * satisfied, which is what lets a legitimately empty master start.
 *
 * <p>Records published after the capture are folded in as they arrive and are not waited for. The
 * gate establishes a floor, it does not chase a live topic.
 *
 * <p>On timeout the application fails to start. It does not release the listener with a partial map:
 * that would trade one loud failure for a quiet stream of dead letters, which is the outcome the
 * gate exists to prevent.
 *
 * <p>A malformed instrument record is logged at ERROR and skipped. There is no
 * {@code reference-data.instruments.dlq} and this service's policy grants none. The consequence is
 * that every trade for that ticker dead-letters with {@code instrument_missing} until a corrected
 * record is published, which is visible in the metric rather than silent.
 *
 * <p><strong>Thread ownership around {@code close()}.</strong> {@link KafkaConsumer} is not
 * thread-safe, and {@code wakeup()} is the only one of its methods documented safe to call from a
 * thread other than the one currently polling. Exactly one thread ever calls {@code poll} at a
 * time: the caller of {@link #loadInitialSnapshot()} while the catch-up loop runs, then the
 * follower thread started by {@link #startFollowing()} once the gate opens. {@code close()} never
 * touches the consumer directly except through {@code wakeup()}; it signals the owning thread to
 * stop, joins it, and only calls {@code consumer.close()} once it has confirmed that thread has
 * actually exited. The {@code closed} flag and the lock guarding it exist to close two windows that
 * timing alone does not close: a {@code close()} that arrives before {@link #startFollowing()} has
 * assigned the {@code follower} field, which must prevent the follower from starting at all rather
 * than starting it unsupervised, and a {@code close()} that arrives while
 * {@link #loadInitialSnapshot()} is still blocked in {@code poll}, which surfaces as a caught
 * {@link WakeupException} and a clean early return rather than an uncaught exception out of a
 * startup path whose only documented failure is {@link IllegalStateException}. If the follower does
 * not exit within the join window, {@code close()} logs a warning and leaves the consumer open
 * rather than closing it underneath a thread that might still be inside {@code poll}.
 */
public class InstrumentCacheLoader implements AutoCloseable {

    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration SHUTDOWN_JOIN = Duration.ofSeconds(5);

    private static final Logger log = LoggerFactory.getLogger(InstrumentCacheLoader.class);

    private final InstrumentCache cache;
    private final Map<String, Object> consumerProperties;
    private final String topic;
    private final Duration timeout;
    private final Runnable onLoaded;
    private final AtomicBoolean loaded = new AtomicBoolean();
    private final Object lifecycleLock = new Object();

    private volatile KafkaConsumer<String, InstrumentReferenceEvent> consumer;
    private volatile Thread follower;
    private volatile boolean running = true;
    private boolean closed = false;

    public InstrumentCacheLoader(InstrumentCache cache,
                                 Map<String, Object> consumerProperties,
                                 String topic,
                                 Duration timeout,
                                 Runnable onLoaded) {
        this.cache = cache;
        this.consumerProperties = consumerProperties;
        this.topic = topic;
        this.timeout = timeout;
        this.onLoaded = onLoaded;
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    /**
     * Blocks until every partition has been read to its captured end offset, then runs the callback
     * that starts the trade listener.
     *
     * <p>If {@link #close()} runs while this is blocked in {@code poll}, the resulting
     * {@link WakeupException} is caught here and treated as a clean shutdown: the method returns
     * without setting {@link #isLoaded()} and without starting the follower, rather than propagating
     * a stack trace out of a startup path.
     *
     * @throws IllegalStateException if the condition is not met within the configured timeout
     */
    public void loadInitialSnapshot() {
        consumer = new KafkaConsumer<>(consumerProperties);
        List<TopicPartition> partitions = partitionsOf(consumer);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        Map<TopicPartition, Long> ends = new HashMap<>(consumer.endOffsets(partitions));
        long deadline = System.nanoTime() + timeout.toNanos();

        while (!caughtUp(ends)) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        "Timed out after " + timeout + " reading the instrument master from " + topic
                                + ". Starting the trade listener now would dead-letter every trade "
                                + "for an instrument this process has not folded yet.");
            }
            try {
                fold(consumer.poll(POLL));
            } catch (WakeupException e) {
                log.info("Instrument master load for {} interrupted by close() before the catch-up "
                        + "condition was met; the trade listener was never started", topic);
                return;
            }
        }

        loaded.set(true);
        log.info("Instrument master loaded from {} partitions={} instruments={}",
                topic, partitions.size(), cache.size());
        onLoaded.run();
        startFollowing();
    }

    /**
     * Keeps folding after the gate opens, so a reference-data update reaches a running service.
     *
     * <p>Starting the thread and assigning {@link #follower} happen under {@link #lifecycleLock},
     * the same lock {@link #close()} takes before it decides whether a follower exists to join. A
     * loader closed after the catch-up loop finished but before this method ran must never start a
     * follower against a consumer that {@code close()} is about to hand off to {@code wakeup()}.
     */
    private void startFollowing() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            follower = Thread.ofVirtual().name("instrument-cache-follower").start(() -> {
                while (running) {
                    try {
                        fold(consumer.poll(POLL));
                    } catch (WakeupException e) {
                        return;
                    } catch (RuntimeException e) {
                        // The gate has already opened, so a failure here degrades freshness rather than
                        // correctness: the map keeps serving what it holds. Killing the thread silently
                        // is the one outcome to avoid.
                        log.error("Instrument master follow failed, the cache is now frozen", e);
                        return;
                    }
                }
            });
        }
    }

    private boolean caughtUp(Map<TopicPartition, Long> ends) {
        return ends.entrySet().stream()
                .allMatch(end -> consumer.position(end.getKey()) >= end.getValue());
    }

    private void fold(ConsumerRecords<String, InstrumentReferenceEvent> records) {
        for (ConsumerRecord<String, InstrumentReferenceEvent> record : records) {
            try {
                cache.apply(record.key(), record.value());
            } catch (RuntimeException e) {
                log.error("Skipping instrument record partition={} offset={} key={}: "
                                + "every trade for its ticker will dead-letter until it is corrected",
                        record.partition(), record.offset(), record.key(), e);
            }
        }
    }

    private List<TopicPartition> partitionsOf(KafkaConsumer<String, InstrumentReferenceEvent> client) {
        List<PartitionInfo> infos = client.partitionsFor(topic);
        if (infos == null || infos.isEmpty()) {
            throw new IllegalStateException("The instrument master topic " + topic + " has no partitions");
        }
        return infos.stream().map(info -> new TopicPartition(topic, info.partition())).toList();
    }

    /**
     * Idempotent: a second call is a no-op. {@code wakeup()} is the only consumer method called
     * from this thread while the follower or the loading thread might still own {@code poll}; the
     * consumer itself is only closed once the follower has been confirmed exited, or was never
     * started at all.
     */
    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        running = false;
        KafkaConsumer<String, InstrumentReferenceEvent> client = consumer;
        if (client != null) {
            client.wakeup();
        }

        Thread thread;
        synchronized (lifecycleLock) {
            thread = follower;
        }
        if (thread != null) {
            try {
                thread.join(SHUTDOWN_JOIN);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (thread.isAlive()) {
                log.warn("Instrument cache follower for {} did not stop within {} of close(); "
                        + "leaving the consumer open rather than closing it under a live thread",
                        topic, SHUTDOWN_JOIN);
                return;
            }
        }
        if (client != null) {
            client.close();
        }
    }
}
