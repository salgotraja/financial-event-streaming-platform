package dev.engnotes.fes.marketdatasimulator;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import dev.engnotes.fes.marketdatasimulator.MarketDataSimulatorProperties.Generation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Paces {@link TickGenerator} into {@link MarketDataTickPublisher} at a configured rate (FR-01.2,
 * FR-01.4).
 *
 * <p><strong>Batched, not one event per wake.</strong> The JVM cannot reliably park for less than a
 * millisecond, so a driver that emitted one event per wake would top out near 1,000 events/sec,
 * a twentieth of the FR-01.4 ceiling. The driver instead wakes on a fixed interval and emits the
 * whole batch due for that interval.
 *
 * <p><strong>Sends are not awaited.</strong> Blocking on each future would serialise the loop on
 * broker round trips and make the configured rate unreachable. Delivery failures surface through the
 * publisher's error callback. The producer's 64MB buffer is the backpressure boundary: once it
 * fills, {@code send} blocks until {@code max.block.ms}, which is the correct signal that the target
 * rate exceeds what the broker will take.
 *
 * <p>A missed deadline is dropped rather than made up. Bursting to catch up after a stall would
 * push a spike into a broker that has already shown it cannot keep pace.
 *
 * <p>Configuring a rate is not measuring one. The NFR-01.1 sustained-throughput result is a Phase 8
 * load test under the production durability profile, and nothing here substitutes for it.
 */
public class TickGenerationDriver implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TickGenerationDriver.class);

    private final TickGenerator generator;
    private final MarketDataTickPublisher publisher;
    private final Generation generation;
    private final int batchSize;
    private final long intervalNanos;
    private final AtomicLong emitted = new AtomicLong();
    private final ReentrantLock lifecycle = new ReentrantLock();

    private volatile boolean running;
    private Thread worker;

    public TickGenerationDriver(TickGenerator generator,
                                MarketDataTickPublisher publisher,
                                Generation generation) {
        this.generator = generator;
        this.publisher = publisher;
        this.generation = generation;
        this.intervalNanos = generation.batchInterval().toNanos();
        this.batchSize = batchSize(generation);
    }

    private static int batchSize(Generation generation) {
        double perInterval = generation.ratePerSecond()
                * (generation.batchInterval().toNanos() / 1_000_000_000d);
        return Math.max(1, (int) Math.round(perInterval));
    }

    @Override
    public void start() {
        lifecycle.lock();
        try {
            if (running) {
                return;
            }
            running = true;
            worker = Thread.ofVirtual().name("tick-generator").start(this::run);
            log.info("Tick generation started ratePerSecond={} batchSize={} batchIntervalMs={}",
                    generation.ratePerSecond(), batchSize, generation.batchInterval().toMillis());
        } finally {
            lifecycle.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycle.lock();
        try {
            if (!running) {
                return;
            }
            running = false;
            LockSupport.unpark(worker);
            worker.join(Duration.ofSeconds(5));
            log.info("Tick generation stopped after {} ticks", emitted.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lifecycle.unlock();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Ticks published since start. Exposed for tests and for a throughput probe. */
    public long emitted() {
        return emitted.get();
    }

    /** Ticks emitted per wake, derived from the target rate and the wake interval. */
    int batchSize() {
        return batchSize;
    }

    private void run() {
        long deadline = System.nanoTime();
        while (running) {
            for (int i = 0; i < batchSize && running; i++) {
                publisher.publish(generator.next());
                emitted.incrementAndGet();
            }

            deadline += intervalNanos;
            long remaining = deadline - System.nanoTime();
            if (remaining > 0) {
                LockSupport.parkNanos(remaining);
            } else {
                // Behind schedule. Resume from now rather than bursting the shortfall.
                deadline = System.nanoTime();
            }
        }
    }
}
