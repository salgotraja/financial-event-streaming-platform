package dev.engnotes.fes.tradeenrichment;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import dev.engnotes.fes.common.cache.MarketCacheKeys;
import dev.engnotes.fes.testing.KafkaAvroStack;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
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
 *
 * <p>{@code spring.kafka.listener.auto-startup=false} alone is not enough to make this context
 * Kafka-free once Task 6 wired the instrument-master readiness gate: that gate is a blocking
 * {@code SmartInitializingSingleton} that runs regardless of any listener's auto-start setting, so
 * this context needs a reachable broker and an empty reference topic, not just a disabled listener.
 * An empty topic loads immediately (its captured end offset already equals its beginning), so this
 * costs the test nothing beyond the broker's own startup time, which {@link KafkaAvroStack} already
 * pays once per JVM for every other module test.
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

    private static final String REFERENCE_TOPIC = "reader-it-ref-" + UUID.randomUUID();

    private static final String TICKER = "reader-it-" + UUID.randomUUID();
    private static final String EXPIRED = "reader-it-expired-" + UUID.randomUUID();

    @Autowired
    private MarketStateReader reader;

    @Autowired
    private StringRedisTemplate redis;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        KafkaAvroStack.start();
        REDIS.start();
        registry.add("spring.kafka.bootstrap-servers", KafkaAvroStack::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaAvroStack::schemaRegistryUrl);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("fes.trade-enrichment-service.reference-topic", () -> REFERENCE_TOPIC);
    }

    @BeforeAll
    static void createTheEmptyReferenceTopic() throws Exception {
        KafkaAvroStack.start();
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAvroStack.bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(REFERENCE_TOPIC, 1, (short) 1))).all().get();
        }
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
