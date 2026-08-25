package dev.engnotes.fes.riskalert.governance;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import dev.engnotes.fes.events.RiskRuleLifecycleEvent;
import dev.engnotes.fes.riskalert.rules.InvalidRuleParametersException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Folds the governed rule history from {@code risk-rules.events} into {@link RiskRuleRegistry}, and
 * decides when the trade listener may start.
 *
 * <p><strong>assign(), not subscribe().</strong> Ordering is per {@code ruleId}, which is the
 * record key, so every version of one rule lands in one partition and the fold does not need
 * cross-partition ordering. Every instance still needs the full governed history, the same reason
 * {@code InstrumentCacheLoader} assigns rather than subscribes, so assigning every partition means
 * no consumer group, no GROUP grant in this service's Kafka policy for the rule topic, and no
 * committed offsets.
 *
 * <p><strong>The termination condition is positional</strong>, exactly as in
 * {@code InstrumentCacheLoader}: {@code position(tp) >= capturedEndOffset(tp)} for every partition,
 * against end offsets captured once at startup. A partition whose captured end equals its
 * beginning is already satisfied, which is what lets a legitimately empty rule topic start, since
 * nothing writes {@code risk-rules.events} until Phase 5.
 *
 * <p>Records published after the capture are folded in as they arrive and are not waited for. The
 * gate establishes a floor, it does not chase a live topic.
 *
 * <p>On timeout the application fails to start. Releasing the listener with a partial fold would
 * evaluate trades against the wrong governed version, or the ungoverned bootstrap set while a
 * governed version was in force, which is a wrong verdict rather than a delayed one.
 *
 * <p><strong>{@code risk-rules.events} is not compacted</strong>, unlike the instrument master.
 * Rule history is deliberately immutable and the topic is retention based, so a null value here is
 * a malformed record rather than a tombstone, and the fold reads full retained history rather than
 * only the latest record per key.
 *
 * <p>There is no dead-letter topic for {@code risk-rules.events} and this service holds no write
 * grant on it. A record that fails to decode, or whose parameters fail validation, is logged,
 * counted through {@code onRejected}, and skipped: it must never fail startup, because the fold
 * gates the listener and a control-plane data error would otherwise become an availability failure
 * of the deterministic streaming plane.
 *
 * <p>Mutation happens only through {@link RiskRuleRegistry#apply(RuleTransition)}, never directly on
 * a {@link RuleTimeline}: the registry owns the concurrency across the fold thread and the listener
 * threads that read it.
 *
 * <p><strong>Thread ownership.</strong> {@link KafkaConsumer} is not thread-safe: every method on
 * it other than {@code wakeup()} must be called by whichever single thread currently owns it, and
 * that includes closing it. Ownership moves exactly once, forward, and the thread holding it at
 * each moment is the only one that ever calls {@code consumer.close()} for it:
 * <ul>
 *   <li>The caller of {@link #loadInitialSnapshot()} owns the consumer for the whole catch-up loop.
 *   If it exits without handing ownership onward, on timeout or because {@link #close()} woke it out
 *   of {@code poll}, it closes the consumer itself before returning or throwing.
 *   <li>If the loop finishes, ownership passes to the follower thread started by
 *   {@link #startFollowing()}, which closes the consumer in a {@code finally} block that runs no
 *   matter why its loop ends, unless {@link #close()} had already run first, in which case no
 *   follower is started at all and the loading thread, still the owner, closes the consumer itself
 *   before returning from {@link #startFollowing()}.
 * </ul>
 * {@code close()} itself never calls anything on the consumer except {@code wakeup()}, which is the
 * one method documented safe to call from outside the owning thread. It sets a {@code closed} flag
 * and reads the {@code follower} field under {@link #lifecycleLock}, the same lock
 * {@link #startFollowing()} takes before starting a follower, so the two can never disagree about
 * whether a follower exists or is about to exist. {@code close()} then joins whatever follower it
 * found; if the follower does not exit within the join window it logs a warning and returns without
 * touching the consumer, because a live thread might still be inside {@code poll}. This makes the
 * invariant structural rather than a narrowed timing window: whichever thread is executing
 * {@code consumer.close()} is always the same thread that was the last to call {@code poll()} on it.
 * The same rule covers setup: if constructing the consumer or capturing its partitions and end
 * offsets fails before the catch-up loop is ever reached, the calling thread is still the only owner
 * there has ever been, so it closes the consumer itself before the exception leaves
 * {@link #loadInitialSnapshot()}. It also covers the {@code onLoaded} callback: that callback runs
 * before {@link #isLoaded()} is allowed to report true and before a follower is considered, so a
 * throwing callback still finds the calling thread as sole owner, closes the consumer itself, and
 * propagates the failure rather than leaving a loader that claims to be ready while the thing meant
 * to make it ready never started.
 */
public class RuleTimelineLoader implements AutoCloseable {

    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration SHUTDOWN_JOIN = Duration.ofSeconds(5);

    private static final Logger log = LoggerFactory.getLogger(RuleTimelineLoader.class);

    private final RiskRuleRegistry registry;
    private final Map<String, Object> consumerProperties;
    private final String topic;
    private final Duration timeout;
    private final Consumer<RuleTransition> validator;
    private final Consumer<String> onRejected;
    private final Runnable onLoaded;
    private final AtomicBoolean loaded = new AtomicBoolean();
    private final Object lifecycleLock = new Object();

    private volatile KafkaConsumer<String, RiskRuleLifecycleEvent> consumer;
    private volatile Thread follower;
    private volatile boolean running = true;
    private boolean closed = false;

    public RuleTimelineLoader(RiskRuleRegistry registry,
                              Map<String, Object> consumerProperties,
                              String topic,
                              Duration timeout,
                              Consumer<RuleTransition> validator,
                              Consumer<String> onRejected,
                              Runnable onLoaded) {
        this.registry = registry;
        this.consumerProperties = consumerProperties;
        this.topic = topic;
        this.timeout = timeout;
        this.validator = validator;
        this.onRejected = onRejected;
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
     * {@link WakeupException} is caught here. This thread still owns the consumer at that point, so
     * it closes it itself before returning, without setting {@link #isLoaded()} and without starting
     * the follower, rather than propagating a stack trace out of a startup path or leaving the
     * consumer for a {@code close()} call that will never touch it directly.
     *
     * @throws IllegalStateException if the condition is not met within the configured timeout
     */
    public void loadInitialSnapshot() {
        consumer = new KafkaConsumer<>(consumerProperties);

        List<TopicPartition> partitions;
        Map<TopicPartition, Long> ends;
        try {
            partitions = partitionsOf(consumer);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            ends = new HashMap<>(consumer.endOffsets(partitions));
        } catch (RuntimeException e) {
            // Nothing has handed ownership onward yet: this thread is the only one that has ever
            // touched the consumer, so it is the one that must close it before the exception leaves.
            consumer.close();
            throw e;
        }

        long deadline = System.nanoTime() + timeout.toNanos();

        try {
            while (!caughtUp(ends)) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(
                            "Timed out after " + timeout + " reading the governed rule history from "
                                    + topic + ". Starting the trade listener now would evaluate trades "
                                    + "against a partial or ungoverned rule set.");
                }
                try {
                    fold(consumer.poll(POLL));
                } catch (WakeupException e) {
                    log.info("Rule timeline load for {} interrupted by close() before the catch-up "
                            + "condition was met; the trade listener was never started", topic);
                    consumer.close();
                    return;
                }
            }
        } catch (RuntimeException e) {
            // Covers both the timeout above and caughtUp() itself, which calls consumer.position()
            // and can throw. Either way this thread is still the sole owner, so it closes the
            // consumer here before the exception leaves, the same as the setup block above.
            consumer.close();
            throw e;
        }

        try {
            onLoaded.run();
        } catch (RuntimeException e) {
            // The callback failing after a successful catch-up is the same hazard as any other
            // exit before ownership transfers: this thread is still the sole owner, and loaded must
            // stay false so a caller can never observe isLoaded() == true for a service that never
            // actually started. No follower either: nothing consumes the registry, so keeping a
            // broker connection alive just to keep it fresh would be pointless.
            consumer.close();
            throw e;
        }

        loaded.set(true);
        log.info("Rule timeline loaded from {} partitions={} timelines={}",
                topic, partitions.size(), registry.timelineCount());
        startFollowing();
    }

    /**
     * Keeps folding after the gate opens, so a governance change reaches a running service.
     *
     * <p>Whether a follower starts at all is decided under {@link #lifecycleLock}, the same lock
     * {@link #close()} takes before it decides whether a follower exists to join. If {@code close()}
     * already ran, no follower is started, and this method closes the consumer itself: the loading
     * thread is still its owner and nothing else will ever close it otherwise. If a follower does
     * start, ownership passes to it, and it closes the consumer in a {@code finally} block that runs
     * regardless of how its loop ends.
     */
    private void startFollowing() {
        boolean loaderClosed;
        synchronized (lifecycleLock) {
            loaderClosed = closed;
            if (!loaderClosed) {
                follower = Thread.ofVirtual().name("rule-timeline-follower").start(() -> {
                    try {
                        while (running) {
                            fold(consumer.poll(POLL));
                        }
                    } catch (WakeupException e) {
                        // close() woke this thread; the finally block below closes the consumer.
                    } catch (RuntimeException e) {
                        // The gate has already opened, so a failure here degrades freshness rather
                        // than correctness: the registry keeps serving what it holds until this
                        // thread exits. Killing the thread silently is the one outcome to avoid.
                        log.error("Rule timeline follow failed, the registry is now frozen", e);
                    } finally {
                        consumer.close();
                    }
                });
            }
        }
        if (loaderClosed) {
            consumer.close();
        }
    }

    private boolean caughtUp(Map<TopicPartition, Long> ends) {
        return ends.entrySet().stream()
                .allMatch(end -> consumer.position(end.getKey()) >= end.getValue());
    }

    private void fold(ConsumerRecords<String, RiskRuleLifecycleEvent> records) {
        for (ConsumerRecord<String, RiskRuleLifecycleEvent> record : records) {
            RiskRuleLifecycleEvent event = record.value();
            if (event == null) {
                // Not a tombstone. risk-rules.events is not compacted and rule history is
                // deliberately immutable, so a null value here is a malformed record and not a
                // deletion; it is skipped.
                log.error("Null value on {} partition={} offset={}. This topic is not compacted, so "
                                + "this is a malformed record and not a deletion; it is skipped.",
                        topic, record.partition(), record.offset());
                onRejected.accept("null_value");
                continue;
            }
            try {
                RuleTransition transition = RuleTransition.of(event);
                validator.accept(transition);
                registry.apply(transition);
            } catch (InvalidRuleParametersException e) {
                // The previously in-force version stays in force. There is no dead letter for this
                // topic and this identity holds no write grant on it, so the metric and this line
                // are the only signal. Failing here would turn a governance data error into an
                // availability failure of the deterministic streaming plane.
                log.error("Rejected governed rule version ruleId={} version={} reason={}: {}",
                        event.getRuleId(), event.getVersion(), e.reason(), e.getMessage());
                onRejected.accept(e.reason());
            } catch (RuntimeException e) {
                log.error("Rejected governed rule version ruleId={} version={} on {} partition={} "
                                + "offset={}", event.getRuleId(), event.getVersion(), topic,
                        record.partition(), record.offset(), e);
                onRejected.accept("malformed_record");
            }
        }
    }

    private List<TopicPartition> partitionsOf(KafkaConsumer<String, RiskRuleLifecycleEvent> client) {
        List<PartitionInfo> infos = client.partitionsFor(topic);
        if (infos == null || infos.isEmpty()) {
            throw new IllegalStateException("The governed rule topic " + topic + " has no partitions");
        }
        return infos.stream().map(info -> new TopicPartition(topic, info.partition())).toList();
    }

    /**
     * Idempotent: a second call is a no-op. This never calls anything on the consumer except
     * {@code wakeup()}; the thread that owns the consumer at the time closes it, as described in the
     * class javadoc. If a follower does not exit within the join window, this leaves the consumer
     * open rather than closing it out from under a thread that might still be inside {@code poll}.
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
        KafkaConsumer<String, RiskRuleLifecycleEvent> client = consumer;
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
                log.warn("Rule timeline follower for {} did not stop within {} of close(); "
                        + "leaving the consumer open rather than closing it under a live thread",
                        topic, SHUTDOWN_JOIN);
            }
        }
    }
}
