package dev.engnotes.fes.riskalert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import dev.engnotes.fes.events.RiskAlertEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Alert-side metrics for this service.
 *
 * <p>Only {@link #recordAlert(RiskAlertEvent)} exists in this increment. Task 10 adds the rejected
 * governed-version counter and the rule-registry gauge to this same class; neither is built here.
 */
@Component
public class RiskAlertMetrics {

    private final MeterRegistry registry;
    private final Map<String, Counter> alerts = new ConcurrentHashMap<>();

    public RiskAlertMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordAlert(RiskAlertEvent alert) {
        String alertType = alert.getAlertType().toString();
        String severity = alert.getSeverity().toString();
        alerts.computeIfAbsent(alertType + ":" + severity, key -> Counter.builder("risk.alerts")
                .tag("alert-type", alertType)
                .tag("severity", severity)
                .description("Alerts published to notifications.alerts, by type and severity")
                .register(registry)).increment();
    }
}
