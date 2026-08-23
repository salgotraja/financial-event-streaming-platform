package dev.engnotes.fes.tradeenrichment;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * The enrichment-side metrics surface. Every method here is a no-op today; Task 7 fills them in.
 *
 * <p>The stub exists this early because the wiring that will eventually call it is Task 6's own
 * work: {@link RawTradeConsumer} calls {@link #recordEnriched} and {@link #recordUnavailable}, and
 * {@link EnrichmentKafkaConfiguration}'s readiness gate calls {@link #bindInstrumentCache} before
 * starting the trade listener. A smaller stub would not compile against that wiring.
 */
@Component
public class EnrichmentMetrics {

    private final MeterRegistry registry;

    public EnrichmentMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordEnriched(EnrichedTradeEvent enriched) {
        // Filled in by Task 7.
    }

    public void recordUnavailable(UnavailableReason reason) {
        // Filled in by Task 7.
    }

    public void recordQuarantined() {
        // Filled in by Task 7.
    }

    public void bindInstrumentCache(InstrumentCache cache) {
        // Filled in by Task 7.
    }
}
