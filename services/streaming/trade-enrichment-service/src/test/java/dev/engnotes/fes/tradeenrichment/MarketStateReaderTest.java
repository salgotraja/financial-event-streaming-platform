package dev.engnotes.fes.tradeenrichment;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MarketStateReader's window fold")
class MarketStateReaderTest {

    private static final long TRADE_AT = 1_740_000_305_000L;   // bucket 1740000300
    private static final long TRADE_BUCKET = 1_740_000_300L;

    @Test
    @DisplayName("should divide the summed price-volume by the summed volume")
    void should_divide_the_summed_price_volume_by_the_summed_volume() {
        Map<String, String> window = Map.of(
                bucket(0) + ":pv", "2000.0", bucket(0) + ":v", "20",
                bucket(-10) + ":pv", "1000.0", bucket(-10) + ":v", "10");

        MarketSnapshot snapshot = MarketStateReader.fold(tick(), window, TRADE_AT);

        assertThat(snapshot.windowPriceVolume()).isEqualTo(3000.0);
        assertThat(snapshot.windowVolume()).isEqualTo(30.0);
        assertThat(MarketStateReader.vwap(snapshot)).isEqualTo(100.0);
    }

    @Test
    @DisplayName("should discard a bucket older than the trade's five-minute horizon")
    void should_discard_a_bucket_older_than_the_trades_five_minute_horizon() {
        Map<String, String> window = Map.of(
                bucket(0) + ":pv", "1000.0", bucket(0) + ":v", "10",
                bucket(-310) + ":pv", "999999.0", bucket(-310) + ":v", "1");

        MarketSnapshot snapshot = MarketStateReader.fold(tick(), window, TRADE_AT);

        assertThat(MarketStateReader.vwap(snapshot)).isEqualTo(100.0);
    }

    @Test
    @DisplayName("should keep the bucket exactly on the lower bound")
    void should_keep_the_bucket_exactly_on_the_lower_bound() {
        Map<String, String> window = Map.of(bucket(-300) + ":pv", "1000.0", bucket(-300) + ":v", "10");

        MarketSnapshot snapshot = MarketStateReader.fold(tick(), window, TRADE_AT);

        assertThat(snapshot.windowVolume()).isEqualTo(10.0);
    }

    @Test
    @DisplayName("should discard a bucket newer than the trade, so replay reproduces the live value")
    void should_discard_a_bucket_newer_than_the_trade_so_replay_reproduces_the_live_value() {
        // The upper bound is the point of the filter. Without it, replaying an old trade against a
        // warm window folds in ticks that arrived after the trade and produces a different vwap5Min
        // than the live run did. Same determinism class as ADR-033's event-time bucketing.
        Map<String, String> window = Map.of(
                bucket(0) + ":pv", "1000.0", bucket(0) + ":v", "10",
                bucket(10) + ":pv", "999999.0", bucket(10) + ":v", "1");

        MarketSnapshot snapshot = MarketStateReader.fold(tick(), window, TRADE_AT);

        assertThat(MarketStateReader.vwap(snapshot)).isEqualTo(100.0);
    }

    @Test
    @DisplayName("should keep the bucket exactly on the upper bound, which is the trade's own")
    void should_keep_the_bucket_exactly_on_the_upper_bound_which_is_the_trades_own() {
        Map<String, String> window = Map.of(bucket(0) + ":pv", "1000.0", bucket(0) + ":v", "10");

        MarketSnapshot snapshot = MarketStateReader.fold(tick(), window, TRADE_AT);

        assertThat(snapshot.windowVolume()).isEqualTo(10.0);
    }

    @Test
    @DisplayName("should ignore lastOffset and any field that is not a bucket")
    void should_ignore_last_offset_and_any_field_that_is_not_a_bucket() {
        // lastOffset is a real field the projector writes into the same hash. Parsing it as a bucket
        // would fold a Kafka offset into a price.
        Map<String, String> window = Map.of(
                bucket(0) + ":pv", "1000.0", bucket(0) + ":v", "10",
                "lastOffset", "4242", "garbage", "7");

        MarketSnapshot snapshot = MarketStateReader.fold(tick(), window, TRADE_AT);

        assertThat(snapshot.windowVolume()).isEqualTo(10.0);
    }

    @Test
    @DisplayName("should report zero volume for an empty window rather than dividing by it")
    void should_report_zero_volume_for_an_empty_window_rather_than_dividing_by_it() {
        MarketSnapshot snapshot = MarketStateReader.fold(tick(), Map.of(), TRADE_AT);

        assertThat(snapshot.windowVolume()).isZero();
    }

    private static String bucket(long offsetSeconds) {
        return Long.toString(TRADE_BUCKET + offsetSeconds);
    }

    private static Map<String, String> tick() {
        return Map.of(
                "eventTimestamp", "1740000304000",
                "bidPrice", "99.0",
                "askPrice", "101.0",
                "lastTradedPrice", "100.0",
                "volume", "5",
                "producedAt", "1740000304100",
                "correlationId", "fold-test");
    }
}
