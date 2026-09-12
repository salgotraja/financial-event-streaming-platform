package dev.engnotes.fes.riskalert.rules;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.common.idempotency.IdempotencyKeys;
import dev.engnotes.fes.events.AlertType;
import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Severity;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.riskalert.correlation.RecentTrade;
import dev.engnotes.fes.riskalert.correlation.RecentTrades;
import dev.engnotes.fes.riskalert.governance.ActiveRule;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SelfCrossRuleTest {

    private static final Map<String, String> BANDS = Map.of(
            SelfCrossParameters.WINDOW_KEY, "300",
            SelfCrossParameters.QUANTITY_TOLERANCE_KEY, "1.0",
            SelfCrossParameters.PRICE_TOLERANCE_KEY, "1.0");

    private static final ActiveRule RULE = new ActiveRule("sc-1", "self-cross", 1, BANDS);

    private static final Instant NOW = Instant.parse("2026-09-12T10:05:00Z");

    private static RecentTrade prior(String tradeId, Side side, long quantity, double price, Instant at) {
        return new RecentTrade(tradeId, 1L, side, quantity, price, at);
    }

    private static Optional<RiskAlertEvent> evaluate(Side side, long quantity, RecentTrade... priors) {
        EnrichedTradeEvent trade = EnrichedTrades.withPosition(
                "t-9", "trader-1", "RELIANCE", side, quantity, NOW);
        return new SelfCrossRule(3_600L).evaluate(trade, RULE,
                new TradeContext(null, null, new RecentTrades(9L, List.of(priors), false)));
    }

    @Test
    void an_offsetting_trade_inside_the_window_and_tolerances_alerts() {
        // EnrichedTrades.withPosition fixes price at 2500.0, so the prior matches on price too.
        assertThat(evaluate(Side.SELL, 100L,
                prior("t-1", Side.BUY, 100L, 2500.0, NOW.minusSeconds(60L))))
                .get()
                .satisfies(alert -> {
                    assertThat(alert.getAlertType()).isEqualTo(AlertType.WASH_TRADE_DETECTED);
                    assertThat(alert.getSeverity()).isEqualTo(Severity.CRITICAL);
                    assertThat(alert.getMeasuredValues()).containsEntry("matched-trade-id", "t-1");
                });
    }

    @Test
    void a_trade_on_the_same_side_is_not_a_cross() {
        assertThat(evaluate(Side.BUY, 100L,
                prior("t-1", Side.BUY, 100L, 2500.0, NOW.minusSeconds(60L))))
                .isEmpty();
    }

    @Test
    void a_prior_outside_the_governed_window_is_not_a_cross() {
        // 301 seconds against a governed window of 300, even though the store offered it as a
        // candidate under the wider configured horizon.
        assertThat(evaluate(Side.SELL, 100L,
                prior("t-1", Side.BUY, 100L, 2500.0, NOW.minusSeconds(301L))))
                .isEmpty();
    }

    @Test
    void a_quantity_outside_the_governed_tolerance_is_not_a_cross() {
        // 102 against 100 is 2 percent, over the governed 1 percent.
        assertThat(evaluate(Side.SELL, 100L,
                prior("t-1", Side.BUY, 102L, 2500.0, NOW.minusSeconds(60L))))
                .isEmpty();
    }

    @Test
    void a_price_outside_the_governed_tolerance_is_not_a_cross() {
        assertThat(evaluate(Side.SELL, 100L,
                prior("t-1", Side.BUY, 100L, 2600.0, NOW.minusSeconds(60L))))
                .isEmpty();
    }

    @Test
    void an_empty_candidate_set_does_not_alert() {
        assertThat(evaluate(Side.SELL, 100L)).isEmpty();
    }

    @Test
    void the_nearest_matching_prior_is_the_one_reported() {
        // Candidates arrive newest first, so the first match is the nearest in arrival order.
        assertThat(evaluate(Side.SELL, 100L,
                prior("t-2", Side.BUY, 100L, 2500.0, NOW.minusSeconds(30L)),
                prior("t-1", Side.BUY, 100L, 2500.0, NOW.minusSeconds(60L))))
                .get()
                .extracting(alert -> alert.getMeasuredValues().get("matched-trade-id"))
                .isEqualTo("t-2");
    }

    @Test
    void the_alert_id_is_derived_from_the_trade_the_rule_and_the_version() {
        RiskAlertEvent alert = evaluate(Side.SELL, 100L,
                prior("t-1", Side.BUY, 100L, 2500.0, NOW.minusSeconds(60L))).orElseThrow();

        assertThat(alert.getAlertId().toString()).isEqualTo(
                IdempotencyKeys.deterministic("t-9", "sc-1", "1").toString());
    }

    @Test
    void the_rule_declares_only_the_recent_trades() {
        assertThat(new SelfCrossRule(3_600L).requires()).containsExactly(StateKind.RECENT_TRADES);
    }
}
