package dev.engnotes.fes.riskalert.rules;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import dev.engnotes.fes.riskalert.position.NetPosition;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PositionLimitRuleTest {

    private static final ActiveRule RULE = new ActiveRule("pl-1", "position-limit", 3L, Map.of(
            PositionLimitParameters.WARN_KEY, "10000",
            PositionLimitParameters.CRITICAL_KEY, "50000"));

    private final PositionLimitRule rule = new PositionLimitRule();

    private static EnrichedTradeEvent trade() {
        return EnrichedTrades.withPosition("t-1", "trader-1", "RELIANCE", Side.BUY, 100L,
                Instant.ofEpochMilli(1_000L));
    }

    private static NetPosition position(long net) {
        return new NetPosition("trader-1", "RELIANCE", net);
    }

    @Test
    void a_position_inside_both_bands_raises_no_alert() {
        assertThat(rule.evaluate(trade(), RULE, position(9_999L))).isEmpty();
    }

    @Test
    void a_position_exactly_at_the_warning_band_does_not_breach() {
        assertThat(rule.evaluate(trade(), RULE, position(10_000L)))
                .as("the rule is a strict exceedance: |net| > threshold")
                .isEmpty();
    }

    @Test
    void a_position_above_the_warning_band_raises_a_warning() {
        RiskAlertEvent alert = rule.evaluate(trade(), RULE, position(10_001L)).orElseThrow();

        assertThat(alert.getSeverity()).isEqualTo(Severity.WARNING);
        assertThat(alert.getAlertType()).isEqualTo(AlertType.POSITION_LIMIT_BREACH);
    }

    @Test
    void a_position_above_the_critical_band_raises_a_critical() {
        RiskAlertEvent alert = rule.evaluate(trade(), RULE, position(50_001L)).orElseThrow();

        assertThat(alert.getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void a_position_exactly_at_the_critical_band_warns_rather_than_criticals() {
        RiskAlertEvent alert = rule.evaluate(trade(), RULE, position(50_000L)).orElseThrow();

        assertThat(alert.getSeverity())
                .as("strict exceedance: exactly at the band is not above it")
                .isEqualTo(Severity.WARNING);
    }

    @Test
    void a_short_position_breaches_at_the_same_magnitude_as_a_long_one() {
        Optional<RiskAlertEvent> shortSide = rule.evaluate(trade(), RULE, position(-50_001L));
        Optional<RiskAlertEvent> longSide = rule.evaluate(trade(), RULE, position(50_001L));

        assertThat(shortSide).isPresent();
        assertThat(shortSide.orElseThrow().getSeverity())
                .isEqualTo(longSide.orElseThrow().getSeverity());
    }

    @Test
    void the_signed_net_is_carried_so_a_short_reads_as_a_short() {
        RiskAlertEvent alert = rule.evaluate(trade(), RULE, position(-50_001L)).orElseThrow();

        assertThat(alert.getMeasuredValues())
                .containsEntry("net-position-quantity", "-50001")
                .containsEntry("net-position-magnitude", "50001");
    }

    @Test
    void the_governed_bands_are_carried_into_the_alert() {
        RiskAlertEvent alert = rule.evaluate(trade(), RULE, position(60_000L)).orElseThrow();

        assertThat(alert.getRuleParameters())
                .containsEntry(PositionLimitParameters.WARN_KEY, "10000")
                .containsEntry(PositionLimitParameters.CRITICAL_KEY, "50000");
        assertThat(alert.getRuleId()).hasToString("pl-1");
        assertThat(alert.getRuleVersion()).isEqualTo(3L);
    }

    @Test
    void the_alert_timestamp_is_the_trades_own_event_time_not_the_wall_clock() {
        RiskAlertEvent alert = rule.evaluate(trade(), RULE, position(60_000L)).orElseThrow();

        assertThat(alert.getAlertTimestamp()).isEqualTo(Instant.ofEpochMilli(1_000L));
    }

    @Test
    void the_alert_id_is_derived_so_a_replay_produces_the_same_id() {
        RiskAlertEvent first = rule.evaluate(trade(), RULE, position(60_000L)).orElseThrow();
        RiskAlertEvent replayed = rule.evaluate(trade(), RULE, position(60_000L)).orElseThrow();

        assertThat(first.getAlertId()).isEqualTo(replayed.getAlertId());
        assertThat(first.getAlertId())
                .hasToString(IdempotencyKeys.deterministic("t-1", "pl-1", "3").toString());
    }

    @Test
    void a_different_governed_version_derives_a_different_alert_id() {
        ActiveRule newerVersion = new ActiveRule("pl-1", "position-limit", 4L, RULE.parameters());

        RiskAlertEvent original = rule.evaluate(trade(), RULE, position(60_000L)).orElseThrow();
        RiskAlertEvent underNewerVersion =
                rule.evaluate(trade(), newerVersion, position(60_000L)).orElseThrow();

        assertThat(original.getAlertId()).isNotEqualTo(underNewerVersion.getAlertId());
    }

    @Test
    void the_trace_context_comes_from_the_wrapped_trade() {
        RiskAlertEvent alert = rule.evaluate(trade(), RULE, position(60_000L)).orElseThrow();

        assertThat(alert.getTraceContext()).containsKey("traceparent");
    }

    @Test
    void invalid_governed_bands_are_rejected_rather_than_silently_skipped() {
        ActiveRule broken = new ActiveRule("pl-broken", "position-limit", 1L,
                Map.of(PositionLimitParameters.WARN_KEY, "10000"));

        assertThatThrownBy(() -> rule.evaluate(trade(), broken, position(60_000L)))
                .isInstanceOf(InvalidRuleParametersException.class);
    }

    @Test
    void the_two_argument_evaluate_is_a_programming_error_not_a_runtime_condition() {
        assertThatThrownBy(() -> ((RiskRule) rule).evaluate(trade(), RULE))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("position");
    }

    @Test
    void the_rule_type_is_the_dispatch_key() {
        assertThat(rule.ruleType()).isEqualTo("position-limit");
    }
}
