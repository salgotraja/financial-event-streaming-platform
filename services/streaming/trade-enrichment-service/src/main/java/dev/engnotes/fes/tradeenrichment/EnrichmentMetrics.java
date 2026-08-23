package dev.engnotes.fes.tradeenrichment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * The read side of the market cache, and the enrichment flow itself.
 *
 * <p>{@code market_cache_stale_reads_total} and {@code market_cache_miss_total} are listed under the
 * projector in specification-v1.2, but they are reader-side quantities and the projector emits
 * neither: it never reads, so a miss counter there would report zero misses when it means no readers.
 * They are satisfied here by {@code enrichment_reference_unavailable_total} tagged {@code stale} and
 * {@code tick_absent}.
 *
 * <p>Summaries rather than timers for the two millisecond quantities. Both are already computed in
 * milliseconds on the event, so a {@code Timer} would only re-derive them into a different unit and
 * publish {@code _seconds} names that the specification does not use.
 *
 * <p>No ticker tag anywhere. Per-ticker cardinality is bounded on the write side by the simulator's
 * ticker set, but the trade flow's ticker universe is not this service's to bound.
 */
@Component
public class EnrichmentMetrics {

    private final MeterRegistry registry;
    private final Counter enriched;
    private final Counter quarantined;
    private final DistributionSummary latency;
    private final DistributionSummary marketDataAge;
    private final Map<UnavailableReason, Counter> unavailable = new ConcurrentHashMap<>();

    public EnrichmentMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.enriched = status("enriched");
        this.quarantined = status("quarantined");
        this.latency = DistributionSummary.builder("enrichment.latency.ms")
                .description("Milliseconds from consume to pre-publish, not to broker acknowledgement")
                .register(registry);
        this.marketDataAge = DistributionSummary.builder("market.data.age.ms")
                .description("Event-time age of the cache entry each trade was enriched against")
                .register(registry);
    }

    public void recordEnriched(EnrichedTradeEvent event) {
        enriched.increment();
        latency.record(event.getEnrichmentLatencyMs());
        marketDataAge.record(event.getMarketDataAgeMs());
    }

    public void recordUnavailable(UnavailableReason reason) {
        unavailable.computeIfAbsent(reason, key -> Counter.builder("enrichment.reference.unavailable")
                .tag("reason", key.label())
                .description("Trades that could not be enriched, by why")
                .register(registry)).increment();
    }

    public void recordQuarantined() {
        quarantined.increment();
    }

    /** Registered by the loader once the initial fold completes, so the gauge never reports a partial map. */
    public void bindInstrumentCache(InstrumentCache cache) {
        Gauge.builder("enrichment.instrument.cache.size", cache, InstrumentCache::size)
                .description("Instruments folded from the compacted master, not a claim it is complete")
                .register(registry);
    }

    private Counter status(String status) {
        return Counter.builder("trades.enriched")
                .tag("status", status)
                .description("Trades leaving this service, enriched or quarantined")
                .register(registry);
    }
}
