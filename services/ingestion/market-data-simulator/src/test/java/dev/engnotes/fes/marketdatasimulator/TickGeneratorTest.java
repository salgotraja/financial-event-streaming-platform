package dev.engnotes.fes.marketdatasimulator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.random.RandomGenerator;
import java.util.stream.IntStream;

import dev.engnotes.fes.events.MarketDataTickEvent;
import dev.engnotes.fes.marketdatasimulator.MarketDataSimulatorProperties.Model;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("TickGenerator")
class TickGeneratorTest {

    private static final Instant NOW = Instant.parse("2026-08-16T09:15:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    @DisplayName("should hold bid <= last traded <= ask on every tick")
    void should_hold_bid_le_last_traded_le_ask_on_every_tick() {
        TickGenerator generator = generator(model(Map.of("RELIANCE", 2_500.00d)), seeded(42L));

        IntStream.range(0, 500).forEach(_ -> {
            MarketDataTickEvent tick = generator.next();
            assertThat(tick.getBidPrice()).isLessThanOrEqualTo(tick.getLastTradedPrice());
            assertThat(tick.getLastTradedPrice()).isLessThanOrEqualTo(tick.getAskPrice());
        });
    }

    @Test
    @DisplayName("should keep the price positive across a long walk")
    void should_keep_the_price_positive_across_a_long_walk() {
        // The exact GBM solution is exp of a normal draw, so it cannot reach zero. The Euler
        // arithmetic form can, which is why this asserts on a long run rather than a single step.
        Model volatile_ = new Model(
                Map.of("RELIANCE", 2_500.00d), 0.08d, 5.0d, 60d, 5.0d, 1.5d, 100L);
        TickGenerator generator = generator(volatile_, seeded(7L));

        IntStream.range(0, 20_000).forEach(_ ->
                assertThat(generator.next().getLastTradedPrice()).isGreaterThan(0d));
    }

    @Test
    @DisplayName("should apply the configured spread symmetrically around the mid")
    void should_apply_the_configured_spread_symmetrically_around_the_mid() {
        TickGenerator generator = generator(model(Map.of("TCS", 3_800.00d)), seeded(1L));

        MarketDataTickEvent tick = generator.next();
        double mid = tick.getLastTradedPrice();

        // 5 bps total, so 2.5 bps either side.
        assertThat(tick.getAskPrice() - mid).isCloseTo(mid * 0.00025d, within());
        assertThat(mid - tick.getBidPrice()).isCloseTo(mid * 0.00025d, within());
    }

    @Test
    @DisplayName("should rotate through every configured instrument")
    void should_rotate_through_every_configured_instrument() {
        TickGenerator generator = generator(
                model(Map.of("RELIANCE", 2_500.00d, "TCS", 3_800.00d, "INFY", 1_500.00d)),
                seeded(3L));

        List<String> seen = IntStream.range(0, 3)
                .mapToObj(_ -> generator.next().getTicker().toString())
                .toList();

        assertThat(seen).containsExactlyInAnyOrder("RELIANCE", "TCS", "INFY");
    }

    @Test
    @DisplayName("should never emit a volume below the Pareto scale")
    void should_never_emit_a_volume_below_the_pareto_scale() {
        TickGenerator generator = generator(model(Map.of("INFY", 1_500.00d)), seeded(11L));

        IntStream.range(0, 1_000).forEach(_ ->
                assertThat(generator.next().getVolume()).isGreaterThanOrEqualTo(100L));
    }

    @Test
    @DisplayName("should produce a heavy volume tail that a thin-tailed draw would not")
    void should_produce_a_heavy_volume_tail_that_a_thin_tailed_draw_would_not() {
        // ADR-006 chose Pareto so volume can spike far enough to exercise the FR-04.2 unusual-volume
        // rule. With alpha 1.5 and x_m 100, P(volume > 100x) is 10^-3, so 20,000 draws clear it
        // comfortably while a Gaussian never would.
        TickGenerator generator = generator(model(Map.of("WIPRO", 450.00d)), seeded(5L));

        long max = IntStream.range(0, 20_000)
                .mapToLong(_ -> generator.next().getVolume())
                .max()
                .orElseThrow();

        assertThat(max).isGreaterThan(10_000L);
    }

    @Test
    @DisplayName("should be reproducible for a given seed")
    void should_be_reproducible_for_a_given_seed() {
        Model model = model(Map.of("RELIANCE", 2_500.00d));

        List<Double> first = walk(generator(model, seeded(99L)));
        List<Double> second = walk(generator(model, seeded(99L)));

        assertThat(first).isEqualTo(second);
    }

    @Test
    @DisplayName("should stamp both timestamps from the injected clock")
    void should_stamp_both_timestamps_from_the_injected_clock() {
        MarketDataTickEvent tick = generator(model(Map.of("TCS", 3_800.00d)), seeded(2L)).next();

        assertThat(tick.getEventTimestamp()).isEqualTo(NOW);
        assertThat(tick.getProducedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("should give every tick a distinct correlation id")
    void should_give_every_tick_a_distinct_correlation_id() {
        TickGenerator generator = generator(model(Map.of("INFY", 1_500.00d)), seeded(4L));

        List<String> ids = IntStream.range(0, 100)
                .mapToObj(_ -> generator.next().getCorrelationId().toString())
                .toList();

        assertThat(ids).doesNotHaveDuplicates();
    }

    private static List<Double> walk(TickGenerator generator) {
        return IntStream.range(0, 50)
                .mapToObj(_ -> generator.next().getLastTradedPrice())
                .toList();
    }

    private static TickGenerator generator(Model model, RandomGenerator random) {
        return new TickGenerator(model, random, FIXED_CLOCK);
    }

    private static Model model(Map<String, Double> instruments) {
        return new Model(instruments, 0.08d, 0.25d, 1.0d, 5.0d, 1.5d, 100L);
    }

    private static RandomGenerator seeded(long seed) {
        return new java.util.Random(seed);
    }

    private static org.assertj.core.data.Offset<Double> within() {
        return org.assertj.core.data.Offset.offset(1e-9d);
    }
}
