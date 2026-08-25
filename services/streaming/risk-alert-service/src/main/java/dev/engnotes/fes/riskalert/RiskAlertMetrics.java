package dev.engnotes.fes.riskalert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Alert, rejection and fold metrics for this service.
 *
 * <p>{@code recordAlert} and {@code recordRejectedRuleVersion} use literal snake_case names and tag
 * keys, matching architecture-v1.2's observability section for {@code risk_alerts_fired_total}, so
 * that a {@code SimpleMeterRegistry} lookup finds them without depending on a naming convention
 * applied only at scrape time. {@code recordQuarantined} keeps the same style for consistency within
 * this class, even though it has no counterpart in the spec: {@code trades.enriched} has a
 * dead-letter path and no reservation for it there, so it is named for what it is.
 */
@Component
public class RiskAlertMetrics {

    private final MeterRegistry registry;
    private final Map<String, Counter> alerts = new ConcurrentHashMap<>();
    private final Map<String, Counter> rejectedRuleVersions = new ConcurrentHashMap<>();
    private final Counter quarantined;

    public RiskAlertMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.quarantined = Counter.builder("trades_enriched_quarantined_total")
                .description("Enriched trades quarantined to trades.enriched.dlq")
                .register(registry);
    }

    public void recordAlert(RiskAlertEvent alert) {
        String alertType = alert.getAlertType().toString();
        String severity = alert.getSeverity().toString();
        alerts.computeIfAbsent(alertType + ":" + severity, key -> Counter.builder("risk_alerts_fired_total")
                .tag("alert_type", alertType)
                .tag("severity", severity)
                .description("Alerts published to notifications.alerts, by type and severity")
                .register(registry)).increment();
    }

    public void recordRejectedRuleVersion(String reason) {
        rejectedRuleVersions.computeIfAbsent(reason, key -> Counter.builder("risk_rule_versions_rejected_total")
                .tag("reason", key)
                .description("Governed rule versions rejected during the fold from risk-rules.events, by reason")
                .register(registry)).increment();
    }

    public void recordQuarantined() {
        quarantined.increment();
    }

    /** Registered by the loader once the initial fold completes, so the gauge never reports a partial fold. */
    public void bindRuleRegistry(RiskRuleRegistry registry) {
        Gauge.builder("risk_rule_timelines", registry, RiskRuleRegistry::timelineCount)
                .description("Rule timelines folded from risk-rules.events, not a claim the fold is complete")
                .register(this.registry);
    }
}
