package dev.engnotes.fes.riskalert.rules;

import dev.engnotes.fes.riskalert.correlation.RecentTrades;
import dev.engnotes.fes.riskalert.position.NetPosition;
import dev.engnotes.fes.riskalert.window.VolumeWindow;

/**
 * The per-trade state the engine applied for one trade, one field per {@link StateKind}.
 *
 * <p>A field is non-null exactly when some rule in force declared its kind. Both the engine's union
 * and its dispatch loop read one registry snapshot, so a rule reached by the loop was seen by the
 * union that decided what to apply: a rule's declared kinds are always non-null when it runs.
 */
public record TradeContext(NetPosition position, VolumeWindow volumeWindow, RecentTrades recentTrades) {
}
