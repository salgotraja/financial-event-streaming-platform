package dev.engnotes.fes.marketdatacacheprojector;

import java.util.Map;

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

/**
 * The compare-and-set contract (ADR-032). Kafka orders ticks within a partition, which covers first
 * delivery and nothing else: a rebalance, an offset rewind or a deliberate rebuild all replay
 * records that a last-write-wins projection would apply as though they were current.
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
        redis.delete(MarketStateProjection.KEY_PREFIX + TICKER);
    }

    @Test
    @DisplayName("should write the entry when no state exists for the ticker")
    void should_write_the_entry_when_no_state_exists_for_the_ticker() {
        assertThat(projection.project(tick(1_000L, 101.5))).isEqualTo(ProjectionOutcome.APPLIED);

        assertThat(stored())
                .containsEntry("lastTradedPrice", "101.5")
                .containsEntry("eventTimestamp", "1000");
    }

    @Test
    @DisplayName("should apply a newer tick over an older one")
    void should_apply_a_newer_tick_over_an_older_one() {
        projection.project(tick(1_000L, 101.5));

        assertThat(projection.project(tick(2_000L, 102.5))).isEqualTo(ProjectionOutcome.APPLIED);
        assertThat(stored()).containsEntry("lastTradedPrice", "102.5");
    }

    @Test
    @DisplayName("should skip a tick whose timestamp equals the stored one")
    void should_skip_a_tick_whose_timestamp_equals_the_stored_one() {
        projection.project(tick(1_000L, 101.5));

        // Redelivery of the same record. One effect, not two, is what makes at-least-once safe here.
        assertThat(projection.project(tick(1_000L, 999.0))).isEqualTo(ProjectionOutcome.DUPLICATE);
        assertThat(stored()).containsEntry("lastTradedPrice", "101.5");
    }

    @Test
    @DisplayName("should skip a tick older than the stored one")
    void should_skip_a_tick_older_than_the_stored_one() {
        projection.project(tick(2_000L, 102.5));

        assertThat(projection.project(tick(1_000L, 101.5))).isEqualTo(ProjectionOutcome.OLDER);
        assertThat(stored())
                .as("a stale price installed silently is the failure this guards")
                .containsEntry("lastTradedPrice", "102.5");
    }

    private Map<Object, Object> stored() {
        return redis.opsForHash().entries(MarketStateProjection.KEY_PREFIX + TICKER);
    }

    private static MarketDataTickEvent tick(long eventTimestampMillis, double lastTradedPrice) {
        return MarketDataTickEvent.newBuilder()
                .setTicker(TICKER)
                .setBidPrice(lastTradedPrice - 0.5)
                .setAskPrice(lastTradedPrice + 0.5)
                .setLastTradedPrice(lastTradedPrice)
                .setVolume(100L)
                .setEventTimestamp(java.time.Instant.ofEpochMilli(eventTimestampMillis))
                .setProducedAt(java.time.Instant.ofEpochMilli(eventTimestampMillis + 5))
                .setCorrelationId("corr-" + eventTimestampMillis)
                .build();
    }
}
