package dev.engnotes.fes.positionexposure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Snapshot and quarantine metrics for this service.
 *
 * <p>Registered with dot-delimited names, matching every meter in {@code RiskAlertMetrics} and its
 * siblings. Production runs on {@code PrometheusMeterRegistry}, whose naming convention renders a
 * dotted name such as {@code position.snapshots.published} as {@code position_snapshots_published_total}:
 * dots become underscores and {@code _total} is appended for a counter. Registering the rendered,
 * already-suffixed form directly would be registering the wire format rather than the convention's
 * input, so the dotted form is what is registered here.
 */
@Component
public class PositionExposureMetrics {

    private final Counter snapshotsPublished;
    private final Counter tradesQuarantined;

    public PositionExposureMetrics(MeterRegistry registry) {
        this.snapshotsPublished = Counter.builder("position.snapshots.published")
                .description("Position snapshots published to positions.snapshots")
                .register(registry);
        this.tradesQuarantined = Counter.builder("position.exposure.trades.quarantined")
                .description("Enriched trades this service quarantined to trades.enriched.dlq")
                .register(registry);
    }

    public void recordSnapshot() {
        snapshotsPublished.increment();
    }

    public void recordQuarantined() {
        tradesQuarantined.increment();
    }
}
