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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

        // The metric name is fixed by architecture-v1.2's observability section, and the business
        // dashboard's "risk alerts by severity" panel reads exactly these two tags.
        assertThat(registry.get("risk_alerts_fired_total")
                .tag("alert_type", "PRICE_DEVIATION")
                .tag("severity", "CRITICAL")
                .counter().count()).isEqualTo(2.0);
        assertThat(registry.get("risk_alerts_fired_total")
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
        // because free text makes the tag unbounded in cardinality.
        assertThat(registry.get("risk_rule_versions_rejected_total")
                .tag("reason", "missing_parameter").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("risk_rule_versions_rejected_total")
                .tag("reason", "unparseable_value").counter().count()).isEqualTo(1.0);
    }

    @Test
    void the_rule_timeline_count_is_exposed_as_a_gauge() {
        RiskRuleRegistry rules = new RiskRuleRegistry(new BootstrapRuleProperties(List.of()));
        metrics.bindRuleRegistry(rules);

        assertThat(registry.get("risk_rule_timelines").gauge().value()).isZero();

        rules.apply(new RuleTransition("pd", "price-deviation", 1, RuleState.ACTIVE,
                Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0"),
                1_000L));

        // A gauge rather than a counter, and read through the live registry rather than captured at
        // bind time: the follower thread keeps folding after startup, so a snapshot would report the
        // startup value forever and hide every runtime rule change.
        assertThat(registry.get("risk_rule_timelines").gauge().value()).isEqualTo(1.0);
    }
}
