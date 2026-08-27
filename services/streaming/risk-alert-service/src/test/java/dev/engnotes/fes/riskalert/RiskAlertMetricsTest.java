package dev.engnotes.fes.riskalert;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.RuleState;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.riskalert.governance.BootstrapRuleProperties;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import dev.engnotes.fes.riskalert.governance.RuleTransition;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RiskAlertMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final RiskAlertMetrics metrics = new RiskAlertMetrics(registry);

    private static RiskAlertEvent alert(AlertType type, Severity severity) {
        return RiskAlertEvent.newBuilder()
                .setAlertId("a-1")
                .setCorrelationId("corr-1")
                .setTriggeringTradeId("trade-1")
                .setAlertType(type)
                .setSeverity(severity)
                .setTicker("RELIANCE")
                .setTraderId("trader-1")
                .setDescription("test")
                .setRuleParameters(Map.of())
                .setMeasuredValues(Map.of())
                .setRuleId("pd")
                .setRuleVersion(1L)
                .setAlertTimestamp(Instant.ofEpochMilli(1_000L))
                .build();
    }

    @Test
    void an_alert_increments_the_counter_tagged_by_type_and_severity() {
        metrics.recordAlert(alert(AlertType.PRICE_DEVIATION, Severity.CRITICAL));
        metrics.recordAlert(alert(AlertType.PRICE_DEVIATION, Severity.WARNING));
        metrics.recordAlert(alert(AlertType.PRICE_DEVIATION, Severity.CRITICAL));

        // Registered dotted, matching both sibling modules: production runs on a
        // PrometheusMeterRegistry, which renders a dotted name as the underscored, "_total"-suffixed
        // form. "risk.alerts.fired" is the registration name; the rendered
        // "risk_alerts_fired_total{alert_type,severity}" architecture-v1.2:510 specifies is proven by
        // the scrape test below, not by this SimpleMeterRegistry lookup, which does no naming
        // translation and can only see the literal registered name.
        assertThat(registry.get("risk.alerts.fired")
                .tag("alert_type", "PRICE_DEVIATION")
                .tag("severity", "CRITICAL")
                .counter().count()).isEqualTo(2.0);
        assertThat(registry.get("risk.alerts.fired")
                .tag("alert_type", "PRICE_DEVIATION")
                .tag("severity", "WARNING")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void a_rejected_rule_version_increments_a_counter_tagged_by_reason() {
        metrics.recordRejectedRuleVersion("missing_parameter");
        metrics.recordRejectedRuleVersion("missing_parameter");
        metrics.recordRejectedRuleVersion("unparseable_value");

        // risk-rules.events has no dead-letter path and this identity holds no write grant on it,
        // so this counter and the ERROR log line are the only signal a governed version was
        // rejected. The tag is a fixed slug from InvalidRuleParametersException, never free text,
        // because free text makes the tag unbounded in cardinality. Registered dotted for the same
        // Prometheus-scrape reason as the alert counter above.
        assertThat(registry.get("risk.rule.versions.rejected")
                .tag("reason", "missing_parameter").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("risk.rule.versions.rejected")
                .tag("reason", "unparseable_value").counter().count()).isEqualTo(1.0);
    }

    @Test
    void the_rule_timeline_count_is_exposed_as_a_gauge() {
        RiskRuleRegistry rules = new RiskRuleRegistry(new BootstrapRuleProperties(List.of()));
        metrics.bindRuleRegistry(rules);

        assertThat(registry.get("risk.rule.timelines").gauge().value()).isZero();

        rules.apply(new RuleTransition("pd", "price-deviation", 1, RuleState.ACTIVE,
                Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0"),
                1_000L));

        // A gauge rather than a counter, and read through the live registry rather than captured at
        // bind time: the follower thread keeps folding after startup, so a snapshot would report the
        // startup value forever and hide every runtime rule change.
        assertThat(registry.get("risk.rule.timelines").gauge().value()).isEqualTo(1.0);
    }

    @Test
    void a_quarantined_trade_increments_its_own_counter() {
        metrics.recordQuarantined();
        metrics.recordQuarantined();

        // Named for what this service measures (poison trades it failed to consume from
        // trades.enriched), distinct from trade-enrichment-service's own trades.enriched counter,
        // which measures a different thing under a name that would otherwise look the same once
        // rendered.
        assertThat(registry.get("risk.alert.trades.quarantined").counter().count()).isEqualTo(2.0);
    }

    /**
     * Proves the rendered, exported form of every meter in this class against a real
     * {@link PrometheusMeterRegistry}, which a {@link SimpleMeterRegistry}-based test can never
     * observe: {@code SimpleMeterRegistry} applies no naming convention, so it cannot tell a
     * correctly dot-registered counter from one registered with the rendered name already on it.
     * Both forms were checked empirically against this module's resolved
     * micrometer-registry-prometheus 1.17.0 / prometheus-metrics-model 1.5.1: neither throws on
     * scrape here, because {@code PrometheusNamingConvention.name()} sanitizes a trailing
     * {@code _total} away before {@code MetricMetadata} is built. That sanitizing strip is an
     * internal implementation detail of the library, not a documented guarantee, so this test still
     * asserts the scrape does not throw and renders the expected names, as the regression check for
     * whichever registration form is in force.
     */
    @Test
    void every_meter_in_this_class_scrapes_through_a_real_prometheus_registry_without_throwing() {
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        RiskAlertMetrics prometheusMetrics = new RiskAlertMetrics(prometheus);
        RiskRuleRegistry rules = new RiskRuleRegistry(new BootstrapRuleProperties(List.of()));

        prometheusMetrics.recordAlert(alert(AlertType.PRICE_DEVIATION, Severity.CRITICAL));
        prometheusMetrics.recordRejectedRuleVersion("missing_parameter");
        prometheusMetrics.recordQuarantined();
        prometheusMetrics.bindRuleRegistry(rules);

        // PrometheusMeterRegistry builds a counter's metadata lazily inside the collector invoked
        // by scrape(), not by the increment calls above, so any naming-related failure surfaces here.
        assertThatCode(prometheus::scrape).doesNotThrowAnyException();

        String scraped = prometheus.scrape();
        assertThat(scraped).contains("risk_alerts_fired_total");
        assertThat(scraped).contains("risk_rule_versions_rejected_total");
        assertThat(scraped).contains("risk_alert_trades_quarantined_total");
        assertThat(scraped).contains("risk_rule_timelines");
    }
}
