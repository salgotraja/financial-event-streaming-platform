package dev.engnotes.fes.tradeproducer;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import java.util.random.RandomGenerator;

import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.tradeproducer.TradeProducerProperties.Generation;

/**
 * Draws a synthetic trade execution (FR-01.1).
 *
 * <p>No price model. The market-data simulator owns the Geometric Brownian Motion walk because
 * FR-01.4 specifies one for ticks; nothing specifies a model for trade prices, and inventing one
 * here would be a claim about market behaviour this platform does not make. A uniform draw around a
 * fixed reference is honest about being arbitrary.
 *
 * <p>{@link RandomGenerator} and {@link Clock} are injected rather than called statically so a test
 * can seed the draw and assert exact values.
 */
public class TradeGenerator {

    private static final double REFERENCE_PRICE = 1_000d;
    private static final double PRICE_SPREAD = 100d;
    private static final long MAX_QUANTITY = 500L;

    private final Generation generation;
    private final RandomGenerator random;
    private final Clock clock;

    public TradeGenerator(Generation generation, RandomGenerator random, Clock clock) {
        this.generation = generation;
        this.random = random;
        this.clock = clock;
    }

    public TradeEvent next() {
        var now = clock.instant();
        String ticker = generation.tickers().get(random.nextInt(generation.tickers().size()));

        return TradeEvent.newBuilder()
                .setTradeId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setTicker(ticker)
                .setQuantity(1L + random.nextLong(MAX_QUANTITY))
                .setPrice(REFERENCE_PRICE + (random.nextDouble() - 0.5d) * PRICE_SPREAD)
                .setSide(random.nextBoolean() ? Side.BUY : Side.SELL)
                .setTraderId("trader-" + random.nextInt(10))
                .setAccountId("account-" + random.nextInt(10))
                .setEventTimestamp(now)
                .setProducedAt(now)
                .setTraceContext(Map.of())
                .build();
    }
}
