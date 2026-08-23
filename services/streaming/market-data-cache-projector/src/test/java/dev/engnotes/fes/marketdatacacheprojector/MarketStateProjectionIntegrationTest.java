package dev.engnotes.fes.marketdatacacheprojector;

import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.common.cache.MarketCacheKeys;
import dev.engnotes.fes.events.MarketDataTickEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two guards, which are deliberately different (ADR-033).
 *
 * <p>The latest-price hash is governed by the source-event timestamp, so a replayed or out-of-order
 * tick can never install a stale price. The window is governed by the Kafka offset instead, because
 * two distinct ticks can share a millisecond and the timestamp guard would drop the second one's
 * volume as though it were a duplicate. Volume that was really traded must count.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false",
        "spring.kafka.listener.auto-startup=false"
})
@DisplayName("MarketStateProjection against a real Redis")
class MarketStateProjectionIntegrationTest {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:8.10.1-alpine")).withExposedPorts(6379);

    private static final String TICKER = "RELIANCE";
    private static final long BASE_MILLIS = 1_740_000_000_000L;

    @Autowired
    private MarketStateProjection projection;

    @Autowired
    private StringRedisTemplate redis;

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        REDIS.start();
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @BeforeEach
    void clearTicker() {
        redis.delete(MarketCacheKeys.tickKey(TICKER));
        redis.delete(MarketCacheKeys.windowKey(TICKER));
    }

    @Test
    @DisplayName("should write the entry when no state exists for the ticker")
    void should_write_the_entry_when_no_state_exists_for_the_ticker() {
        assertThat(projection.project(tick(BASE_MILLIS, 101.5, 10L), 1L).outcome())
                .isEqualTo(ProjectionOutcome.APPLIED);

        assertThat(tickState())
                .containsEntry("lastTradedPrice", "101.5")
                .containsEntry("eventTimestamp", Long.toString(BASE_MILLIS));
    }

    @Test
    @DisplayName("should apply a newer tick over an older one")
    void should_apply_a_newer_tick_over_an_older_one() {
        projection.project(tick(BASE_MILLIS, 101.5, 10L), 1L);

        assertThat(projection.project(tick(BASE_MILLIS + 1_000, 102.5, 10L), 2L).outcome())
                .isEqualTo(ProjectionOutcome.APPLIED);
        assertThat(tickState()).containsEntry("lastTradedPrice", "102.5");
    }

    @Test
    @DisplayName("should skip a tick whose timestamp equals the stored one")
    void should_skip_a_tick_whose_timestamp_equals_the_stored_one() {
        projection.project(tick(BASE_MILLIS, 101.5, 10L), 1L);

        assertThat(projection.project(tick(BASE_MILLIS, 999.0, 10L), 2L).outcome())
                .isEqualTo(ProjectionOutcome.DUPLICATE);
        assertThat(tickState()).containsEntry("lastTradedPrice", "101.5");
    }

    @Test
    @DisplayName("should skip a tick older than the stored one")
    void should_skip_a_tick_older_than_the_stored_one() {
        projection.project(tick(BASE_MILLIS + 1_000, 102.5, 10L), 1L);

        assertThat(projection.project(tick(BASE_MILLIS, 101.5, 10L), 2L).outcome())
                .isEqualTo(ProjectionOutcome.OLDER);
        assertThat(tickState())
                .as("a stale price installed silently is the failure this guards")
                .containsEntry("lastTradedPrice", "102.5");
    }

    @Test
    @DisplayName("should place a tick in the bucket its own event timestamp selects")
    void should_place_a_tick_in_the_bucket_its_own_event_timestamp_selects() {
        projection.project(tick(BASE_MILLIS, 100.0, 3L), 1L);

        long bucket = BASE_MILLIS / 1000 / 10 * 10;
        assertThat(windowState())
                .containsEntry(bucket + ":pv", "300")
                .containsEntry(bucket + ":v", "3");
    }

    @Test
    @DisplayName("should not double count when the same offset is replayed")
    void should_not_double_count_when_the_same_offset_is_replayed() {
        projection.project(tick(BASE_MILLIS, 100.0, 3L), 7L);

        ProjectionResult replay = projection.project(tick(BASE_MILLIS, 100.0, 3L), 7L);

        assertThat(replay.windowApplied())
                .as("a redelivered record must not add its volume a second time")
                .isFalse();
        long bucket = BASE_MILLIS / 1000 / 10 * 10;
        assertThat(windowState()).containsEntry(bucket + ":v", "3");
    }

    @Test
    @DisplayName("should count both ticks when two distinct ticks share a millisecond")
    void should_count_both_ticks_when_two_distinct_ticks_share_a_millisecond() {
        projection.project(tick(BASE_MILLIS, 100.0, 3L), 1L);

        ProjectionResult second = projection.project(tick(BASE_MILLIS, 100.0, 4L), 2L);

        // The timestamp guard calls this a DUPLICATE, correctly, for the latest-price hash. The
        // volume is still real and must reach the window, which is why the window has its own guard.
        assertThat(second.outcome()).isEqualTo(ProjectionOutcome.DUPLICATE);
        assertThat(second.windowApplied()).isTrue();
        long bucket = BASE_MILLIS / 1000 / 10 * 10;
        assertThat(windowState()).containsEntry(bucket + ":v", "7");
    }

    @Test
    @DisplayName("should prune a bucket older than the window from the incoming tick")
    void should_prune_a_bucket_older_than_the_window_from_the_incoming_tick() {
        projection.project(tick(BASE_MILLIS, 100.0, 3L), 1L);
        long oldBucket = BASE_MILLIS / 1000 / 10 * 10;

        // Six minutes later, so the first bucket falls outside the five-minute window.
        projection.project(tick(BASE_MILLIS + 360_000, 110.0, 2L), 2L);

        assertThat(windowState())
                .doesNotContainKey(oldBucket + ":pv")
                .doesNotContainKey(oldBucket + ":v");
    }

    @Test
    @DisplayName("should report how many buckets the window holds")
    void should_report_how_many_buckets_the_window_holds() {
        projection.project(tick(BASE_MILLIS, 100.0, 3L), 1L);
        ProjectionResult second = projection.project(tick(BASE_MILLIS + 20_000, 100.0, 3L), 2L);

        assertThat(second.windowBuckets()).isEqualTo(2);
    }

    @Test
    @DisplayName("should give the window a ttl so an idle ticker does not linger forever")
    void should_give_the_window_a_ttl_so_an_idle_ticker_does_not_linger_forever() {
        projection.project(tick(BASE_MILLIS, 100.0, 3L), 1L);

        assertThat(redis.getExpire(MarketCacheKeys.windowKey(TICKER)))
                .as("the latest-price key deliberately has none; an empty window is honestly empty")
                .isPositive();
        assertThat(redis.getExpire(MarketCacheKeys.tickKey(TICKER))).isNegative();
    }

    @Test
    @DisplayName("should write both keys in one call so they cannot disagree")
    void should_write_both_keys_in_one_call_so_they_cannot_disagree() {
        projection.project(tick(BASE_MILLIS, 100.0, 3L), 42L);

        assertThat(tickState())
                .as("the tick hash must reflect the tick just projected")
                .containsEntry("eventTimestamp", Long.toString(BASE_MILLIS));
        assertThat(windowState())
                .as("the window must reflect the same call, by the offset it was given")
                .containsEntry("lastOffset", "42");
    }

    @Test
    @DisplayName("should reject a tick carrying a non-finite price and leave redis untouched")
    void should_reject_a_tick_carrying_a_non_finite_price_and_leave_redis_untouched() {
        MarketDataTickEvent nanTick = MarketDataTickEvent.newBuilder()
                .setTicker(TICKER)
                .setBidPrice(Double.NaN)
                .setAskPrice(102.0)
                .setLastTradedPrice(101.5)
                .setVolume(10L)
                .setEventTimestamp(Instant.ofEpochMilli(BASE_MILLIS))
                .setProducedAt(Instant.ofEpochMilli(BASE_MILLIS + 5))
                .setCorrelationId("corr-nan")
                .build();

        assertThatThrownBy(() -> projection.project(nanTick, 1L))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tickState()).isEmpty();
        assertThat(windowState()).isEmpty();
    }

    @Test
    @DisplayName("should reject a tick carrying a negative volume")
    void should_reject_a_tick_carrying_a_negative_volume() {
        assertThatThrownBy(() -> projection.project(tick(BASE_MILLIS, 100.0, -1L), 1L))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tickState()).isEmpty();
        assertThat(windowState()).isEmpty();
    }

    @Test
    @DisplayName("should reject a tick whose event timestamp is more than an hour ahead of the clock")
    void should_reject_a_tick_whose_event_timestamp_is_more_than_an_hour_ahead_of_the_clock() {
        long skewedMillis = Instant.now().plusSeconds(4_000).toEpochMilli();

        assertThatThrownBy(() -> projection.project(tick(skewedMillis, 100.0, 3L), 1L))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(windowState()).isEmpty();
    }

    private Map<Object, Object> tickState() {
        return redis.opsForHash().entries(MarketCacheKeys.tickKey(TICKER));
    }

    private Map<Object, Object> windowState() {
        return redis.opsForHash().entries(MarketCacheKeys.windowKey(TICKER));
    }

    private static MarketDataTickEvent tick(long eventTimestampMillis, double lastTradedPrice, long volume) {
        return MarketDataTickEvent.newBuilder()
                .setTicker(TICKER)
                .setBidPrice(lastTradedPrice - 0.5)
                .setAskPrice(lastTradedPrice + 0.5)
                .setLastTradedPrice(lastTradedPrice)
                .setVolume(volume)
                .setEventTimestamp(Instant.ofEpochMilli(eventTimestampMillis))
                .setProducedAt(Instant.ofEpochMilli(eventTimestampMillis + 5))
                .setCorrelationId("corr-" + eventTimestampMillis)
                .build();
    }
}
