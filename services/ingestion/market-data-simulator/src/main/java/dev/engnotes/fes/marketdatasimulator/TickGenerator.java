package dev.engnotes.fes.marketdatasimulator;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.random.RandomGenerator;

import dev.engnotes.fes.events.MarketDataTickEvent;
import dev.engnotes.fes.marketdatasimulator.MarketDataSimulatorProperties.Model;

/**
 * Generates price ticks with Geometric Brownian Motion and Pareto-distributed volume (ADR-006).
 *
 * <p>The price step is the exact GBM solution, {@code S·exp((mu - sigma^2/2)·dt + sigma·sqrt(dt)·Z)},
 * not the Euler arithmetic approximation {@code S·(1 + mu·dt + sigma·sqrt(dt)·Z)}. The arithmetic
 * form can produce a negative price on a large downward draw, which is not a price at all, and it
 * omits the Ito correction so the series drifts away from the configured mu.
 *
 * <p>Volume is drawn by inverse CDF, {@code x_m / U^(1/alpha)}, with alpha below 2 and therefore
 * infinite variance. The upper tail is deliberately uncapped: heavy-tailed volume is the whole reason
 * ADR-006 chose Pareto over a Gaussian, since a thin-tailed draw would never exercise the FR-04.2
 * unusual-volume rule.
 *
 * <p>Not thread safe. The price walk is mutable per-instrument state, and one driver thread owns it.
 *
 * <p>This model is not a claim about market microstructure fidelity. No number it produces may be
 * presented as a market observation (ADR-006).
 */
public class TickGenerator {

    /** 252 trading days, 6.5 hours each. The unit that annualised drift and volatility are quoted in. */
    private static final double TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600;

    private final Model model;
    private final RandomGenerator random;
    private final Clock clock;

    private final List<String> tickers;
    private final double[] prices;
    private final double dt;
    private final double sqrtDt;
    private final double halfSpread;
    private final double inverseShape;

    private int cursor;

    public TickGenerator(Model model, RandomGenerator random, Clock clock) {
        this.model = model;
        this.random = random;
        this.clock = clock;
        this.tickers = List.copyOf(model.instruments().keySet());
        this.prices = tickers.stream().mapToDouble(model.instruments()::get).toArray();
        this.dt = model.stepSeconds() / TRADING_SECONDS_PER_YEAR;
        this.sqrtDt = Math.sqrt(dt);
        this.halfSpread = model.spreadBps() / 10_000d / 2d;
        this.inverseShape = 1d / model.paretoShape();
    }

    /** Advances the walk for the next instrument in rotation and returns its tick. */
    public MarketDataTickEvent next() {
        int index = cursor;
        cursor = (cursor + 1) % tickers.size();

        double mid = advance(index);
        Instant now = clock.instant();

        return MarketDataTickEvent.newBuilder()
                .setTicker(tickers.get(index))
                .setBidPrice(mid * (1 - halfSpread))
                .setAskPrice(mid * (1 + halfSpread))
                .setLastTradedPrice(mid)
                .setVolume(nextVolume())
                .setEventTimestamp(now)
                .setProducedAt(now)
                .setCorrelationId(UUID.randomUUID().toString())
                .setTraceContext(Map.of())
                .build();
    }

    private double advance(int index) {
        double drift = (model.driftAnnual() - (model.volatilityAnnual() * model.volatilityAnnual()) / 2) * dt;
        double shock = model.volatilityAnnual() * sqrtDt * random.nextGaussian();
        double next = prices[index] * Math.exp(drift + shock);
        prices[index] = next;
        return next;
    }

    private long nextVolume() {
        // nextDouble() is [0,1). A zero draw would divide by zero, so the smallest positive double is
        // substituted, which keeps the tail open rather than truncating it.
        double u = random.nextDouble();
        if (u == 0d) {
            u = Double.MIN_NORMAL;
        }
        double volume = model.paretoScale() / Math.pow(u, inverseShape);
        return volume >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) volume;
    }
}
