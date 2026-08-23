package dev.engnotes.fes.common.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MarketCacheKeys, the contract between the projector and its readers")
class MarketCacheKeysTest {

    @Test
    @DisplayName("should brace the ticker so both keys share a cluster hash tag")
    void should_brace_the_ticker_so_both_keys_share_a_cluster_hash_tag() {
        assertThat(MarketCacheKeys.tickKey("RELIANCE")).isEqualTo("market:{RELIANCE}:tick");
        assertThat(MarketCacheKeys.windowKey("RELIANCE")).isEqualTo("market:{RELIANCE}:window");
    }

    @Test
    @DisplayName("should floor a timestamp to its ten-second bucket, in epoch seconds")
    void should_floor_a_timestamp_to_its_ten_second_bucket_in_epoch_seconds() {
        // 1740000007123ms is 1740000007s, which floors to the 1740000000 bucket.
        assertThat(MarketCacheKeys.bucketFor(1_740_000_007_123L)).isEqualTo(1_740_000_000L);
        assertThat(MarketCacheKeys.bucketFor(1_740_000_000_000L)).isEqualTo(1_740_000_000L);
        assertThat(MarketCacheKeys.bucketFor(1_740_000_009_999L)).isEqualTo(1_740_000_000L);
        assertThat(MarketCacheKeys.bucketFor(1_740_000_010_000L)).isEqualTo(1_740_000_010L);
    }

    @Test
    @DisplayName("should floor rather than truncate, so a pre-epoch timestamp matches the Lua script")
    void should_floor_rather_than_truncate_so_a_pre_epoch_timestamp_matches_the_lua_script() {
        // project-tick.lua uses math.floor, which rounds towards negative infinity. Java's integer
        // division truncates towards zero, so -1500ms would land in bucket 0 instead of -10. No
        // production timestamp is negative, but a divergence here would be a silent wrong answer
        // rather than a failure, which is exactly the class of bug this shared class exists to stop.
        assertThat(MarketCacheKeys.bucketFor(-1_500L)).isEqualTo(-10L);
    }

    @Test
    @DisplayName("should pin the horizon and bucket size the projector and its readers both use")
    void should_pin_the_horizon_and_bucket_size_the_projector_and_its_readers_both_use() {
        assertThat(MarketCacheKeys.BUCKET_SECONDS).isEqualTo(10);
        assertThat(MarketCacheKeys.WINDOW_SECONDS).isEqualTo(300);
    }
}
