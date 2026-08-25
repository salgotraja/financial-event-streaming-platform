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
 * <p>Counters are registered with dot-delimited names, matching every meter in
 * {@code EnrichmentMetrics} and {@code MarketCacheMetrics}. Production runs on
 * {@code PrometheusMeterRegistry} (via {@code spring-boot-starter-actuator}, pulled in through
 * {@code platform-common}), whose naming convention renders a dotted name such as
 * {@code risk.alerts.fired} as {@code risk_alerts_fired_total}: dots become underscores and
 * {@code _total} is appended for a counter. {@code risk_alerts_fired_total} in
 * architecture-v1.2.md:510 documents that rendered wire format, not a literal argument to
 * {@code Counter.builder}.
 *
 * <p>Registering the rendered, already-suffixed name directly, as this class did before, is not
 * provably broken against the exact dependency versions resolved here (micrometer-registry-prometheus
 * 1.17.0 / prometheus-metrics-model 1.5.1): {@code PrometheusNamingConvention.name()} calls
 * {@code PrometheusNaming.sanitizeMetricName} on its result, which strips a trailing {@code _total}
 * before {@code MetricMetadata} is ever built, so no exception was observed on scrape in this
 * version (verified with {@code RiskAlertMetricsTest}'s {@code PrometheusMeterRegistry} scrape test,
 * run against both forms). That self-healing is an internal implementation detail of the sanitizer,
 * not a documented contract, and the class comment in {@code PrometheusNamingConvention} itself
 * describes an underscored name as the exact case a naming convention exists to avoid. Dotted
 * registration is kept as the correct, convention-following form regardless: it matches both sibling
 * modules, and {@code PrometheusNaming}'s own javadoc notes that dots are preserved rather than
 * converted when the same meters are exported in OpenTelemetry format.
 *
 * <p>Tag keys are plain, already-valid Prometheus label names ({@code alert_type}, {@code severity},
 * {@code reason}), same as the labels architecture-v1.2.md:510 specifies for the rendered output; no
 * dot-to-underscore conversion applies to them because they contain no dots to convert.
 */
@Component
public class RiskAlertMetrics {

    private final MeterRegistry registry;
    private final Map<String, Counter> alerts = new ConcurrentHashMap<>();
    private final Map<String, Counter> rejectedRuleVersions = new ConcurrentHashMap<>();
    private final Counter quarantined;

    public RiskAlertMetrics(MeterRegistry registry) {
        this.registry = registry;
        // Named for what this service measures: enriched trades it failed to consume from its own
        // input, not the enrichment service's own trades.enriched{status=quarantined} counter,
        // which is a different measurement under a confusingly similar name.
        this.quarantined = Counter.builder("risk.alert.trades.quarantined")
                .description("Enriched trades this service quarantined to trades.enriched.dlq")
                .register(registry);
    }

    public void recordAlert(RiskAlertEvent alert) {
        String alertType = alert.getAlertType().toString();
        String severity = alert.getSeverity().toString();
        alerts.computeIfAbsent(alertType + ":" + severity, key -> Counter.builder("risk.alerts.fired")
                .tag("alert_type", alertType)
                .tag("severity", severity)
                .description("Alerts published to notifications.alerts, by type and severity")
                .register(registry)).increment();
    }

    public void recordRejectedRuleVersion(String reason) {
        rejectedRuleVersions.computeIfAbsent(reason, key -> Counter.builder("risk.rule.versions.rejected")
                .tag("reason", key)
                .description("Governed rule versions rejected during the fold from risk-rules.events, by reason")
                .register(registry)).increment();
    }

    public void recordQuarantined() {
        quarantined.increment();
    }

    /** Registered by the loader once the initial fold completes, so the gauge never reports a partial fold. */
    public void bindRuleRegistry(RiskRuleRegistry registry) {
        Gauge.builder("risk.rule.timelines", registry, RiskRuleRegistry::timelineCount)
                .description("Rule timelines folded from risk-rules.events, not a claim the fold is complete")
                .register(this.registry);
    }
}
