package dev.engnotes.fes.riskalert.rules;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.RiskAlertMetrics;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import dev.engnotes.fes.riskalert.window.VolumeWindow;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class UnusualVolumeRuleTest {

    private static final Map<String, String> BANDS = Map.of(
            UnusualVolumeParameters.WARN_KEY, "3.0",
            UnusualVolumeParameters.CRITICAL_KEY, "5.0",
            UnusualVolumeParameters.MIN_SAMPLES_KEY, "30");

    private static final ActiveRule RULE = new ActiveRule("uv-1", "unusual-volume", 1, BANDS);

    /** A window with mean 100 and population standard deviation 10, over n samples. */
    private static VolumeWindow window(long n) {
        BigDecimal mean = BigDecimal.valueOf(100L);
        BigDecimal sum = mean.multiply(BigDecimal.valueOf(n));
        // Sx2 = n * (variance + mean^2) = n * (100 + 10000)
        BigDecimal sumsq = BigDecimal.valueOf(n).multiply(BigDecimal.valueOf(10_100L));
        return new VolumeWindow("RELIANCE", n, sum, sumsq);
    }

    private static Optional<RiskAlertEvent> evaluate(long quantity, VolumeWindow window) {
        return new UnusualVolumeRule(new RiskAlertMetrics(new SimpleMeterRegistry())).evaluate(
                EnrichedTrades.withPosition("t-1", "trader-1", "RELIANCE", Side.BUY, quantity,
                        Instant.ofEpochMilli(2_000L)),
                RULE,
                new TradeContext(null, window, null));
    }

    @Test
    void a_quantity_below_the_warning_multiplier_does_not_alert() {
        // mean 100 + 3 * 10 = 130. 129 is under it.
        assertThat(evaluate(129L, window(100L))).isEmpty();
    }

    @Test
    void a_quantity_exactly_at_the_warning_multiplier_does_not_alert() {
        // Strict exceedance, matching PositionLimitRule and the plain reading of "exceeds".
        assertThat(evaluate(130L, window(100L))).isEmpty();
    }

    @Test
    void a_quantity_above_the_warning_multiplier_alerts_as_a_warning() {
        assertThat(evaluate(131L, window(100L)))
                .get()
                .satisfies(alert -> {
                    assertThat(alert.getAlertType()).isEqualTo(AlertType.UNUSUAL_VOLUME);
                    assertThat(alert.getSeverity()).isEqualTo(Severity.WARNING);
                });
    }

    @Test
    void a_quantity_above_the_critical_multiplier_alerts_as_critical() {
        // mean 100 + 5 * 10 = 150.
        assertThat(evaluate(151L, window(100L)))
                .get()
                .extracting(RiskAlertEvent::getSeverity)
                .isEqualTo(Severity.CRITICAL);
    }

    @Test
    void a_window_below_the_governed_minimum_sample_does_not_alert_at_any_quantity() {
        // 29 samples against a governed minimum of 30. A three-sigma claim from a thin window is
        // noise, and emitting it would be an unearned statistical claim (ADR-024).
        assertThat(evaluate(1_000_000L, window(29L))).isEmpty();
    }

    @Test
    void a_window_exactly_at_the_governed_minimum_sample_is_allowed_to_alert() {
        // 30 samples against a governed minimum of 30: the guard is sampleCount < minSampleCount,
        // so equality must be on the alerting side, not the suppressed side.
        assertThat(evaluate(1_000_000L, window(30L))).isPresent();
    }

    @Test
    void an_empty_window_does_not_alert() {
        assertThat(evaluate(1_000_000L, new VolumeWindow("RELIANCE", 0L, BigDecimal.ZERO, BigDecimal.ZERO)))
                .isEmpty();
    }

    @Test
    void a_window_below_the_governed_minimum_sample_increments_the_below_minimum_counter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RiskAlertMetrics metrics = new RiskAlertMetrics(registry);

        new UnusualVolumeRule(metrics).evaluate(
                EnrichedTrades.withPosition("t-1", "trader-1", "RELIANCE", Side.BUY, 1_000_000L,
                        Instant.ofEpochMilli(2_000L)),
                RULE,
                new TradeContext(null, window(29L), null));

        assertThat(registry.get("risk.volume.window.below.minimum.sample").counter().count()).isEqualTo(1.0);
    }

    @Test
    void the_alert_carries_the_sample_the_claim_rests_on() {
        assertThat(evaluate(151L, window(100L)))
                .get()
                .extracting(RiskAlertEvent::getMeasuredValues)
                .satisfies(measured -> assertThat(measured)
                        .containsEntry("window-sample-count", "100")
                        .containsEntry("window-mean-quantity", "100")
                        .containsEntry("triggering-trade-quantity", "151"));
    }

    @Test
    void the_alert_id_is_derived_from_the_trade_the_rule_and_the_version() {
        RiskAlertEvent alert = evaluate(151L, window(100L)).orElseThrow();

        assertThat(alert.getAlertId().toString()).isEqualTo(
                IdempotencyKeys.deterministic("t-1", "uv-1", "1").toString());
    }

    @Test
    void the_alert_timestamp_is_the_trade_event_time_rather_than_the_wall_clock() {
        RiskAlertEvent alert = evaluate(151L, window(100L)).orElseThrow();

        assertThat(alert.getAlertTimestamp()).isEqualTo(Instant.ofEpochMilli(2_000L));
    }

    @Test
    void the_rule_declares_only_the_volume_window() {
        assertThat(new UnusualVolumeRule(new RiskAlertMetrics(new SimpleMeterRegistry())).requires())
                .containsExactly(StateKind.VOLUME_WINDOW);
    }
}
