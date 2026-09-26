package dev.engnotes.fes.riskalert.correlation;

import java.util.List;

/**
 * The candidate prior trades for one trade's self-cross evaluation, plus this trade's own arrival
 * order.
 *
 * <p>{@code truncated} is true when the candidate query hit its row cap. A cap that silently
 * dropped the offsetting trade would produce no alert and no signal, so the condition is carried
 * out of the store and counted rather than discarded.
 */
public record RecentTrades(long appliedSeq, List<RecentTrade> priorTrades, boolean truncated) {
}
