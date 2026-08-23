package dev.engnotes.fes.tradeenrichment;

import java.time.Instant;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EnrichmentMetrics")
class EnrichmentMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final EnrichmentMetrics metrics = new EnrichmentMetrics(registry);

    @Test
    @DisplayName("should count an enriched trade and record its latency and market data age")
    void should_count_an_enriched_trade_and_record_its_latency_and_market_data_age() {
        metrics.recordEnriched(enriched(7L, 1_500L));

        assertThat(registry.get("trades.enriched").tag("status", "enriched").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.get("enrichment.latency.ms").summary().totalAmount()).isEqualTo(7.0);
        assertThat(registry.get("market.data.age.ms").summary().totalAmount()).isEqualTo(1500.0);
    }

    @Test
    @DisplayName("should count each unavailable reason under its own tag")
    void should_count_each_unavailable_reason_under_its_own_tag() {
        // The five reasons are separately actionable: tick_absent is a cold cache, stale is a
        // stalled feed, and instrument_missing is a reference-data gap. One undifferentiated
        // counter would make all three look the same on a dashboard.
        metrics.recordUnavailable(UnavailableReason.STALE);
        metrics.recordUnavailable(UnavailableReason.STALE);
        metrics.recordUnavailable(UnavailableReason.TICK_ABSENT);

        assertThat(counter("stale")).isEqualTo(2.0);
        assertThat(counter("tick_absent")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("should count a quarantine under the same meter as an enrichment")
    void should_count_a_quarantine_under_the_same_meter_as_an_enrichment() {
        // specification-v1.2 line 511 names trades_enriched_total{status}. Two meters would make the
        // quarantine rate impossible to express as a fraction of the flow.
        metrics.recordQuarantined();

        assertThat(registry.get("trades.enriched").tag("status", "quarantined").counter().count())
                .isEqualTo(1.0);
    }

    private double counter(String reason) {
        return registry.get("enrichment.reference.unavailable")
                .tag("reason", reason).counter().count();
    }

    private static EnrichedTradeEvent enriched(long latencyMs, long ageMs) {
        TradeEvent trade = TradeEvent.newBuilder()
                .setTradeId("T-1").setCorrelationId("C-1").setTicker("RELIANCE")
                .setQuantity(10L).setPrice(105.0).setSide(Side.BUY)
                .setTraderId("TR-1").setAccountId("AC-1")
                .setEventTimestamp(Instant.ofEpochMilli(1_000L))
                .setProducedAt(Instant.ofEpochMilli(1_005L)).build();
        return EnrichedTradeEvent.newBuilder()
                .setTrade(trade).setMidPriceAtExecution(100.0).setSpreadAtExecution(2.0)
                .setVwap5Min(100.0).setMarketCap(10.0).setPriceDeviation(5.0)
                .setEnrichedAt(Instant.ofEpochMilli(1_010L))
                .setEnrichmentLatencyMs(latencyMs).setMarketDataAgeMs(ageMs).build();
    }
}
