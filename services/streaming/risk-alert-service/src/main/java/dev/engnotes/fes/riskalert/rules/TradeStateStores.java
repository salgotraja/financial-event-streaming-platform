package dev.engnotes.fes.riskalert.rules;

import dev.engnotes.fes.riskalert.correlation.RiskRecentTradeStore;
import dev.engnotes.fes.riskalert.position.RiskPositionStore;
import dev.engnotes.fes.riskalert.window.RiskVolumeWindowStore;

/**
 * The stores the engine can apply, one per {@link StateKind}.
 *
 * <p>A record rather than three constructor arguments, so adding a fourth store in a later
 * increment does not change {@code RiskRuleEngine}'s signature or every test that builds one.
 */
public record TradeStateStores(RiskPositionStore positions,
                               RiskVolumeWindowStore volumeWindows,
                               RiskRecentTradeStore recentTrades) {
}
