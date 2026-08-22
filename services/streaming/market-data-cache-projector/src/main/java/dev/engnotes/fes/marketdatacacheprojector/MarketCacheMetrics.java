package dev.engnotes.fes.marketdatacacheprojector;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * The write-side view of the market cache.
 *
 * <p>The two read-side metrics the specification lists under this service,
 * {@code market_cache_stale_reads_total} and {@code market_cache_miss_total}, belong to trade
 * enrichment. They describe a reader, and a miss counter that nothing can increment reads as "zero
 * misses" when it means "no readers".
 *
 * <p><strong>Gauge, not Timer, for the lag.</strong> A Micrometer {@code Timer} publishes
 * {@code _seconds_count}, {@code _seconds_sum} and {@code _seconds_max}, and never the bare
 * {@code market_cache_projection_lag_seconds} the specification names. Lag is a level rather than an
 * event duration, so nothing is lost by measuring it as one.
 *
 * <p><strong>The gauge values are held in fields.</strong> Micrometer holds the observed object
 * weakly, so a gauge registered over a captured local reports {@code NaN} once that local is
 * collected, and a NaN age looks like a healthy feed on a dashboard.
 *
 * <p>Per-ticker cardinality is bounded by the simulator's ticker set. An unbounded ticker universe
 * would make the age gauge a cardinality problem and would need a different instrument.
 *
 * <p>{@code market.cache.window.buckets} reports how many buckets the rolling window holds per
 * ticker, so pruning is observable, and {@code market.cache.window.skipped} counts a tick the
 * window's offset guard rejected.
 */
@Component
public class MarketCacheMetrics {

    private final MeterRegistry registry;
    private final Clock clock;
    private final AtomicLong projectionLagMillis = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> storedEventTimestamps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> windowBuckets = new ConcurrentHashMap<>();
    private final Counter duplicateWrites;
    private final Counter olderWrites;
    private final Counter windowSkipped;

    public MarketCacheMetrics(MeterRegistry registry, Clock clock) {
        this.registry = registry;
        this.clock = clock;

        Gauge.builder("market.cache.projection.lag.seconds", projectionLagMillis,
                        millis -> millis.get() / 1000.0)
                .description("How far behind the source event the projection is running")
                .register(registry);

        this.duplicateWrites = staleWrites("duplicate");
        this.olderWrites = staleWrites("older");
        this.windowSkipped = Counter.builder("market.cache.window.skipped")
                .description("Ticks the window's offset guard rejected as already applied")
                .register(registry);
    }

    public void record(String ticker, long eventTimestampMillis, ProjectionResult result) {
        switch (result.outcome()) {
            case APPLIED -> {
                projectionLagMillis.set(clock.millis() - eventTimestampMillis);
                storedTimestampFor(ticker).set(eventTimestampMillis);
            }
            case DUPLICATE -> duplicateWrites.increment();
            // Deliberately does not touch the age gauge: the stored entry is the older tick's
            // predecessor, and an age computed from a rejected tick reports the cache as fresher
            // than it is.
            case OLDER -> olderWrites.increment();
        }

        if (result.windowApplied()) {
            windowBucketsFor(ticker).set(result.windowBuckets());
        } else {
            windowSkipped.increment();
        }
    }

    private AtomicLong storedTimestampFor(String ticker) {
        return storedEventTimestamps.computeIfAbsent(ticker, name -> {
            AtomicLong holder = new AtomicLong();
            Gauge.builder("market.cache.entry.age.seconds", holder,
                            stored -> (clock.millis() - stored.get()) / 1000.0)
                    .tag("ticker", name)
                    .description("Age of the stored entry, so a stalled feed is visible")
                    .register(registry);
            return holder;
        });
    }

    private AtomicLong windowBucketsFor(String ticker) {
        return windowBuckets.computeIfAbsent(ticker, name -> {
            AtomicLong holder = new AtomicLong();
            Gauge.builder("market.cache.window.buckets", holder, AtomicLong::get)
                    .tag("ticker", name)
                    .description("Buckets the rolling window holds, so pruning is observable")
                    .register(registry);
            return holder;
        });
    }

    private Counter staleWrites(String reason) {
        return Counter.builder("market.cache.stale.writes")
                .tag("reason", reason)
                .description("Ticks the compare-and-set declined to apply")
                .register(registry);
    }
}
