package dev.engnotes.fes.riskalert.rules;

import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;

/**
 * Shared enriched-trade fixture builder for the rule engine tests. Not a production class.
 */
final class EnrichedTrades {

    private EnrichedTrades() {
    }

    static EnrichedTradeEvent withDeviationAt(double deviation, Instant eventTimestamp) {
        TradeEvent trade = TradeEvent.newBuilder()
                .setTradeId("trade-1")
                .setCorrelationId("corr-1")
                .setTicker("RELIANCE")
                .setQuantity(100L)
                .setPrice(2500.0)
                .setSide(Side.BUY)
                .setTraderId("trader-1")
                .setAccountId("account-1")
                .setEventTimestamp(eventTimestamp)
                .setProducedAt(eventTimestamp.plusMillis(1L))
                .setTraceContext(Map.of("traceparent",
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
                .build();

        return EnrichedTradeEvent.newBuilder()
                .setTrade(trade)
                .setMidPriceAtExecution(2450.0)
                .setSpreadAtExecution(0.5)
                .setVwap5Min(2460.0)
                .setMarketCap(1_700_000.0)
                .setPriceDeviation(deviation)
                .setEnrichedAt(eventTimestamp.plusMillis(2L))
                .setEnrichmentLatencyMs(1L)
                .setMarketDataAgeMs(50L)
                .build();
    }
}
