package dev.engnotes.fes.riskalert.correlation;

import java.time.Instant;

import dev.engnotes.fes.events.Side;

/** One prior trade by the same trader on the same ticker, as a self-cross candidate. */
public record RecentTrade(String tradeId,
                          long appliedSeq,
                          Side side,
                          long quantity,
                          double price,
                          Instant eventTimestamp) {
}
