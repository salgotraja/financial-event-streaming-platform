package dev.engnotes.fes.audit;

import java.time.Instant;

import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;

/** Test data factory, so no test builds a fifteen-argument constructor call by hand. */
final class Trades {

    static final Instant EXECUTED_AT = Instant.parse("2026-08-16T09:15:00Z");

    private Trades() {
    }

    static TradeEvent trade(String tradeId) {
        return TradeEvent.newBuilder()
                .setTradeId(tradeId)
                .setCorrelationId("corr-" + tradeId)
                .setTicker("RELIANCE")
                .setQuantity(100L)
                .setPrice(1450.25)
                .setSide(Side.BUY)
                .setTraderId("TRD-1")
                .setAccountId("ACC-1")
                .setEventTimestamp(EXECUTED_AT)
                .setProducedAt(EXECUTED_AT)
                .build();
    }
}
