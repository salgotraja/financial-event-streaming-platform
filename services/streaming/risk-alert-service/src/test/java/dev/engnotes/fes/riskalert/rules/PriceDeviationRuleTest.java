package dev.engnotes.fes.riskalert.rules;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PriceDeviationRuleTest {

    private final PriceDeviationRule rule = new PriceDeviationRule();

    private static final ActiveRule GOVERNED = new ActiveRule("pd", "price-deviation", 3,
            Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0"));

    private static EnrichedTradeEvent tradeWithDeviation(double deviation) {
        return EnrichedTrades.withDeviationAt(deviation, Instant.ofEpochMilli(1_000L));
    }

    @Test
    void a_deviation_below_the_warning_band_produces_no_alert() {
        assertThat(rule.evaluate(tradeWithDeviation(1.9), GOVERNED)).isEmpty();
    }

    @Test
    void a_deviation_at_the_warning_band_produces_a_warning() {
        Optional<RiskAlertEvent> alert = rule.evaluate(tradeWithDeviation(2.0), GOVERNED);

        assertThat(alert).get().extracting(RiskAlertEvent::getSeverity).isEqualTo(Severity.WARNING);
    }

    @Test
    void a_deviation_at_the_critical_band_produces_a_critical() {
        Optional<RiskAlertEvent> alert = rule.evaluate(tradeWithDeviation(5.0), GOVERNED);

        assertThat(alert).get().extracting(RiskAlertEvent::getSeverity).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void a_negative_deviation_of_the_same_magnitude_alerts_identically() {
        // A trade 6 percent below the mid-price is as far off market as one 6 percent above it.
        // Comparing the signed value would leave every downward breach unalerted.
        assertThat(rule.evaluate(tradeWithDeviation(-6.0), GOVERNED)).get()
                .extracting(RiskAlertEvent::getSeverity).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void a_non_finite_deviation_is_rejected_rather_than_evaluated() {
        // NaN fails every comparison, so it would silently produce no alert. Quarantining it makes
        // the bad record visible instead of losing it.
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> rule.evaluate(tradeWithDeviation(Double.NaN), GOVERNED));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> rule.evaluate(tradeWithDeviation(Double.POSITIVE_INFINITY), GOVERNED));
    }

    @Test
    void the_alert_carries_the_governed_rule_identity_and_version() {
        RiskAlertEvent alert = rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow();

        assertThat(alert.getRuleId()).hasToString("pd");
        assertThat(alert.getRuleVersion()).isEqualTo(3L);
    }

    @Test
    void the_alert_carries_both_bands_and_the_measured_value() {
        RiskAlertEvent alert = rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow();

        assertThat(alert.getRuleParameters())
                .containsEntry("warn-deviation-percent", "2.0")
                .containsEntry("critical-deviation-percent", "5.0");
        assertThat(alert.getMeasuredValues())
                .containsEntry("price-deviation-percent", "6.0")
                .containsEntry("mid-price-at-execution", "2450.0");
    }

    @Test
    void the_alert_carries_the_triggering_trade_and_its_correlation_id() {
        RiskAlertEvent alert = rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow();

        assertThat(alert.getTriggeringTradeId()).hasToString("trade-1");
        assertThat(alert.getCorrelationId()).hasToString("corr-1");
        assertThat(alert.getTicker()).hasToString("RELIANCE");
        assertThat(alert.getTraderId()).hasToString("trader-1");
    }

    @Test
    void the_alert_id_is_the_same_on_every_evaluation_of_the_same_trade_and_rule_version() {
        // Redelivery is normal under at-least-once (ADR-019). A random id would make one breach
        // look like two to every downstream consumer.
        RiskAlertEvent first = rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow();
        RiskAlertEvent second = rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow();

        assertThat(first.getAlertId()).isEqualTo(second.getAlertId());
    }

    @Test
    void a_different_rule_version_produces_a_different_alert_id() {
        ActiveRule version4 = new ActiveRule("pd", "price-deviation", 4, GOVERNED.parameters());

        assertThat(rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow().getAlertId())
                .isNotEqualTo(rule.evaluate(tradeWithDeviation(6.0), version4).orElseThrow().getAlertId());
    }

    @Test
    void the_trace_context_is_carried_from_the_trade_onto_the_alert() {
        // The field defaults to an empty map, so a missing setter compiles and publishes silently.
        RiskAlertEvent alert = rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow();

        assertThat(alert.getTraceContext()).isNotEmpty();
    }

    @Test
    void the_alert_timestamp_is_the_trades_event_time_not_the_wall_clock() {
        // Everything else in this service is event time. A wall-clock alertTimestamp would make a
        // replayed alert differ from the original in the only field a reader uses to order alerts.
        RiskAlertEvent alert = rule.evaluate(tradeWithDeviation(6.0), GOVERNED).orElseThrow();

        assertThat(alert.getAlertTimestamp()).isEqualTo(Instant.ofEpochMilli(1_000L));
    }

    @Test
    void invalid_governed_parameters_surface_as_a_rule_parameter_failure() {
        ActiveRule broken = new ActiveRule("pd", "price-deviation", 3, Map.of("warn-deviation-percent", "2.0"));

        assertThatExceptionOfType(InvalidRuleParametersException.class)
                .isThrownBy(() -> rule.evaluate(tradeWithDeviation(6.0), broken));
    }
}
