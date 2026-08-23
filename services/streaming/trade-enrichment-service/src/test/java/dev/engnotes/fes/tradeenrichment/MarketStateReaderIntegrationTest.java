package dev.engnotes.fes.tradeenrichment;

import java.util.Map;
import java.util.UUID;

import dev.engnotes.fes.common.cache.MarketCacheKeys;
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

/**
 * The script against a real Redis: both keys read in one call, an absent tick reported as empty
 * rather than as a zeroed snapshot, and an expired window folded as empty rather than failing the
 * read.
 */
@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.otlp.tracing.export.enabled=false",
        "spring.kafka.listener.auto-startup=false"
})
@DisplayName("MarketStateReader against a real Redis")
class MarketStateReaderIntegrationTest {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:8.10.1-alpine")).withExposedPorts(6379);

    private static final String TICKER = "reader-it-" + UUID.randomUUID();
    private static final String EXPIRED = "reader-it-expired-" + UUID.randomUUID();

    @Autowired
    private MarketStateReader reader;

    @Autowired
    private StringRedisTemplate redis;

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        REDIS.start();
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Test
    @DisplayName("should return empty when no tick has ever been projected for the ticker")
    void should_return_empty_when_no_tick_has_ever_been_projected_for_the_ticker() {
        assertThat(reader.read("NEVER-SEEN", 1_740_000_000_000L)).isEmpty();
    }

    @Test
    @DisplayName("should read both keys in one call and return the tick beside the folded window")
    void should_read_both_keys_in_one_call_and_return_the_tick_beside_the_folded_window() {
        redis.opsForHash().putAll(MarketCacheKeys.tickKey(TICKER), Map.of(
                "eventTimestamp", "1740000304000", "bidPrice", "99.0", "askPrice", "101.0",
                "lastTradedPrice", "100.0", "volume", "5",
                "producedAt", "1740000304100", "correlationId", "read-it"));
        redis.opsForHash().putAll(MarketCacheKeys.windowKey(TICKER), Map.of(
                "1740000300:pv", "2000.0", "1740000300:v", "20", "lastOffset", "7"));

        MarketSnapshot snapshot = reader.read(TICKER, 1_740_000_305_000L).orElseThrow();

        assertThat(snapshot.eventTimestampMillis()).isEqualTo(1_740_000_304_000L);
        assertThat(snapshot.bidPrice()).isEqualTo(99.0);
        assertThat(snapshot.askPrice()).isEqualTo(101.0);
        assertThat(snapshot.lastTradedPrice()).isEqualTo(100.0);
        assertThat(snapshot.windowVolume()).isEqualTo(20.0);
    }

    @Test
    @DisplayName("should return a tick with an empty window when the window key has expired")
    void should_return_a_tick_with_an_empty_window_when_the_window_key_has_expired() {
        // The window carries a 600s TTL and the tick key carries none, so this pairing is the normal
        // state of an idle ticker rather than an exotic one.
        redis.opsForHash().putAll(MarketCacheKeys.tickKey(EXPIRED), Map.of(
                "eventTimestamp", "1740000304000", "bidPrice", "99.0", "askPrice", "101.0",
                "lastTradedPrice", "100.0", "volume", "5",
                "producedAt", "1740000304100", "correlationId", "read-it"));

        MarketSnapshot snapshot = reader.read(EXPIRED, 1_740_000_305_000L).orElseThrow();

        assertThat(snapshot.windowVolume()).isZero();
    }
}
