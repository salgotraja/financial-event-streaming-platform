package dev.engnotes.fes.tradeenrichment;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("TradeEnricher")
class TradeEnricherTest {

    private static final String TICKER = "RELIANCE";
    private static final long TRADE_AT = 1_740_000_305_000L;
    private static final Duration MAX_AGE = Duration.ofSeconds(30);

    private MarketStateReader reader;
    private InstrumentCache instruments;
    private TradeEnricher enricher;

    @BeforeEach
    void setUp() {
        reader = mock(MarketStateReader.class);
        instruments = new InstrumentCache();
        enricher = new TradeEnricher(reader, instruments, MAX_AGE,
                Clock.fixed(Instant.ofEpochMilli(TRADE_AT + 50), ZoneOffset.UTC));
    }

    @Test
    @DisplayName("should compute every derived field from the one snapshot")
    void should_compute_every_derived_field_from_the_one_snapshot() {
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 2000.0, 20.0), 6_766_000_000L);

        EnrichedTradeEvent enriched = enricher.enrich(trade(105.0), TRADE_AT - 20);

        assertThat(enriched.getMidPriceAtExecution()).isEqualTo(100.0);
        assertThat(enriched.getSpreadAtExecution()).isEqualTo(2.0);
        assertThat(enriched.getVwap5Min()).isEqualTo(100.0);
        assertThat(enriched.getPriceDeviation()).isEqualTo(5.0);
        assertThat(enriched.getMarketDataAgeMs()).isEqualTo(1_000L);
        assertThat(enriched.getTrade().getTradeId()).isEqualTo("T-1");
    }

    @Test
    @DisplayName("should report market capitalisation in crores from the last traded price")
    void should_report_market_capitalisation_in_crores_from_the_last_traded_price() {
        // Bid/ask asymmetric on purpose: mid is 100.0 but lastTradedPrice is 105.0, so this test
        // fails if marketCap is ever computed from mid instead. 1,000,000 shares at 105 INR is
        // 105,000,000 INR, which is 10.5 crore; the mid-price would silently produce 10.0 instead.
        // Do not "tidy" these back to symmetric bid/ask/last, that would erase the discrimination.
        given(snapshot(TRADE_AT - 1_000, 98.0, 102.0, 105.0, 2000.0, 20.0), 1_000_000L);

        EnrichedTradeEvent enriched = enricher.enrich(trade(105.0), TRADE_AT - 20);

        assertThat(enriched.getMarketCap()).isEqualTo(10.5);
    }

    @Test
    @DisplayName("should stamp the latency from the consume start, not from the event timestamp")
    void should_stamp_the_latency_from_the_consume_start_not_from_the_event_timestamp() {
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        EnrichedTradeEvent enriched = enricher.enrich(trade(105.0), TRADE_AT + 20);

        // Clock is TRADE_AT + 50, consume started at TRADE_AT + 20.
        assertThat(enriched.getEnrichmentLatencyMs()).isEqualTo(30L);
        assertThat(enriched.getEnrichedAt()).isEqualTo(Instant.ofEpochMilli(TRADE_AT + 50));
    }

    @Test
    @DisplayName("should accept an age exactly on the maximum")
    void should_accept_an_age_exactly_on_the_maximum() {
        given(snapshot(TRADE_AT - 30_000, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        assertThat(enricher.enrich(trade(105.0), TRADE_AT).getMarketDataAgeMs()).isEqualTo(30_000L);
    }

    @Test
    @DisplayName("should reject an age one millisecond over the maximum as stale")
    void should_reject_an_age_one_millisecond_over_the_maximum_as_stale() {
        given(snapshot(TRADE_AT - 30_001, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(ReferenceDataUnavailableException.class)
                .extracting("reason").isEqualTo(UnavailableReason.STALE);
    }

    @Test
    @DisplayName("should accept an age of exactly zero")
    void should_accept_an_age_of_exactly_zero() {
        given(snapshot(TRADE_AT, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        assertThat(enricher.enrich(trade(105.0), TRADE_AT).getMarketDataAgeMs()).isZero();
    }

    @Test
    @DisplayName("should reject a cache entry newer than the trade rather than enriching from the future")
    void should_reject_a_cache_entry_newer_than_the_trade_rather_than_enriching_from_the_future() {
        // A negative age means the cached tick postdates the trade, which is what replaying an old
        // trade against a warm cache produces. An upper-bound-only policy passes it trivially and
        // silently enriches with market data from after the trade executed (ADR-034).
        given(snapshot(TRADE_AT + 1, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(ReferenceDataUnavailableException.class)
                .extracting("reason").isEqualTo(UnavailableReason.FUTURE);
    }

    @Test
    @DisplayName("should reject a trade whose ticker has never been projected")
    void should_reject_a_trade_whose_ticker_has_never_been_projected() {
        when(reader.read(eq(TICKER), anyLong())).thenReturn(Optional.empty());
        instruments.apply("INE-1", referenceEvent(1_000_000L));

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(ReferenceDataUnavailableException.class)
                .extracting("reason").isEqualTo(UnavailableReason.TICK_ABSENT);
    }

    @Test
    @DisplayName("should reject a window with no volume rather than dividing by zero")
    void should_reject_a_window_with_no_volume_rather_than_dividing_by_zero() {
        // vwap5Min is a non-optional double, so an empty window has no representable value. It also
        // cannot be told apart from a window that idled past its 600s TTL and is repopulating, which
        // is a known limit rather than something this check resolves.
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 0.0, 0.0), 1_000_000L);

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(ReferenceDataUnavailableException.class)
                .extracting("reason").isEqualTo(UnavailableReason.WINDOW_EMPTY);
    }

    @Test
    @DisplayName("should reject a trade for a ticker the instrument master does not carry")
    void should_reject_a_trade_for_a_ticker_the_instrument_master_does_not_carry() {
        when(reader.read(eq(TICKER), anyLong()))
                .thenReturn(Optional.of(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 2000.0, 20.0)));

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(ReferenceDataUnavailableException.class)
                .extracting("reason").isEqualTo(UnavailableReason.INSTRUMENT_MISSING);
    }

    @Test
    @DisplayName("should reject a zero mid-price before it makes priceDeviation infinite")
    void should_reject_a_zero_mid_price_before_it_makes_price_deviation_infinite() {
        // priceDeviation is a non-optional double. The same class of bug as the NaN that reached the
        // projector's Lua multiplication, caught the same way: before the value is constructed.
        given(snapshot(TRADE_AT - 1_000, 0.0, 0.0, 100.0, 2000.0, 20.0), 1_000_000L);

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mid-price");
    }

    @Test
    @DisplayName("should reject a non-finite cached last traded price rather than publishing a NaN market cap")
    void should_reject_a_non_finite_cached_last_traded_price_rather_than_publishing_a_nan_market_cap() {
        // Double.parseDouble("NaN") succeeds, so a corrupt lastTradedPrice reaches the snapshot
        // without a NumberFormatException. Bid/ask are finite so only lastTradedPrice is at fault.
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, Double.NaN, 2000.0, 20.0), 1_000_000L);

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lastTradedPrice");
    }

    @Test
    @DisplayName("should reject a NaN window volume as window_empty rather than publishing a NaN vwap")
    void should_reject_a_nan_window_volume_as_window_empty_rather_than_publishing_a_nan_vwap() {
        // windowVolume() <= 0.0 is false for NaN, so a corrupt volume would otherwise slip past the
        // window_empty guard entirely and vwap5Min would publish as NaN.
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 2000.0, Double.NaN), 1_000_000L);

        assertThatThrownBy(() -> enricher.enrich(trade(105.0), TRADE_AT))
                .isInstanceOf(ReferenceDataUnavailableException.class)
                .extracting("reason").isEqualTo(UnavailableReason.WINDOW_EMPTY);
    }

    @Test
    @DisplayName("should reject a trade carrying a non-finite price")
    void should_reject_a_trade_carrying_a_non_finite_price() {
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        assertThatThrownBy(() -> enricher.enrich(trade(Double.NaN), TRADE_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-finite price");
    }

    @Test
    @DisplayName("should reject a trade carrying a negative quantity")
    void should_reject_a_trade_carrying_a_negative_quantity() {
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        TradeEvent negative = TradeEvent.newBuilder(trade(105.0)).setQuantity(-1L).build();

        assertThatThrownBy(() -> enricher.enrich(negative, TRADE_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative quantity");
    }

    @Test
    @DisplayName("should preserve the original trade unmodified on the enriched event")
    void should_preserve_the_original_trade_unmodified_on_the_enriched_event() {
        given(snapshot(TRADE_AT - 1_000, 99.0, 101.0, 100.0, 2000.0, 20.0), 1_000_000L);

        TradeEvent original = trade(105.0);

        assertThat(enricher.enrich(original, TRADE_AT).getTrade()).isEqualTo(original);
    }

    private void given(MarketSnapshot snapshot, long sharesOutstanding) {
        when(reader.read(eq(TICKER), anyLong())).thenReturn(Optional.of(snapshot));
        instruments.apply("INE-1", referenceEvent(sharesOutstanding));
    }

    private static MarketSnapshot snapshot(long eventTimestampMillis, double bid, double ask,
                                           double last, double priceVolume, double volume) {
        return new MarketSnapshot(eventTimestampMillis, bid, ask, last, priceVolume, volume);
    }

    private static dev.engnotes.fes.events.InstrumentReferenceEvent referenceEvent(long shares) {
        return dev.engnotes.fes.events.InstrumentReferenceEvent.newBuilder()
                .setInstrumentId("INE-1").setTicker(TICKER).setExchange("NSE").setIsin("INE-1")
                .setSecurityType("EQUITY").setCurrency("INR").setSector("ENERGY")
                .setSharesOutstanding(shares).setReferenceVersion(1L)
                .setEffectiveAt(Instant.ofEpochMilli(1_000L))
                .setProducerIdentity("reference-data-service").build();
    }

    private static TradeEvent trade(double price) {
        return TradeEvent.newBuilder()
                .setTradeId("T-1").setCorrelationId("C-1").setTicker(TICKER)
                .setQuantity(10L).setPrice(price).setSide(Side.BUY)
                .setTraderId("TR-1").setAccountId("AC-1")
                .setEventTimestamp(Instant.ofEpochMilli(TRADE_AT))
                .setProducedAt(Instant.ofEpochMilli(TRADE_AT + 5))
                .build();
    }
}
