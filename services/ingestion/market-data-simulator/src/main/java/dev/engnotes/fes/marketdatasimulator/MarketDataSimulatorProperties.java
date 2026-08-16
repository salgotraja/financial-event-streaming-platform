package dev.engnotes.fes.marketdatasimulator;

import java.time.Duration;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Defaults live in the compact constructors rather than in {@code @DefaultValue} annotations, so the
 * bound path and the programmatic path cannot drift apart.
 *
 * @param topic      destination topic. The workload identity for this service is authorised to write
 *                   this topic and nothing else, so changing it requires an IAM policy change too.
 * @param generation tick generation, off unless explicitly enabled
 * @param model      the GBM and Pareto parameters (ADR-006)
 */
@ConfigurationProperties(prefix = "fes.market-data-simulator")
public record MarketDataSimulatorProperties(
        @DefaultValue("market-data.ticks") String topic,
        Generation generation,
        Model model) {

    public MarketDataSimulatorProperties {
        generation = generation == null ? Generation.defaults() : generation;
        model = model == null ? Model.defaults() : model;
    }

    /**
     * @param enabled       FR-01.4 calls this "load simulation mode", so it is a mode rather than
     *                      boot behaviour. Off by default: a service that starts flooding a topic on
     *                      deploy is a surprise, and integration tests that scan the topic for one
     *                      record would have to sift generated traffic.
     * @param ratePerSecond target production rate. FR-01.4 requires 1,000 to 50,000; lower values are
     *                      allowed for local runs and tests.
     * @param batchInterval how often the driver wakes. Millisecond-resolution scheduling caps a
     *                      one-event-per-wake driver near 1,000/sec, well short of the FR-01.4
     *                      ceiling, so the driver emits a batch per wake instead.
     */
    public record Generation(boolean enabled, int ratePerSecond, Duration batchInterval) {

        public Generation {
            if (ratePerSecond <= 0) {
                throw new IllegalArgumentException("ratePerSecond must be positive: " + ratePerSecond);
            }
            if (batchInterval == null || batchInterval.isZero() || batchInterval.isNegative()) {
                throw new IllegalArgumentException("batchInterval must be positive: " + batchInterval);
            }
        }

        static Generation defaults() {
            return new Generation(false, 1_000, Duration.ofMillis(10));
        }
    }

    /**
     * @param instruments      ticker to opening price in INR. The universe the walk runs over.
     * @param driftAnnual      GBM drift, annualised.
     * @param volatilityAnnual GBM volatility, annualised.
     * @param stepSeconds      trading seconds each tick advances the walk. Decoupled from the
     *                         production rate on purpose: raising throughput should not silently
     *                         change the volatility of the generated series.
     * @param spreadBps        bid-ask spread in basis points, split either side of the mid.
     * @param paretoShape      Pareto alpha. ADR-006 fixes 1.5, below 2 and therefore infinite
     *                         variance; that heavy tail is what exercises the FR-04.2 unusual-volume
     *                         rule, so the upper tail is deliberately not capped.
     * @param paretoScale      Pareto x_m, the minimum volume.
     */
    public record Model(
            Map<String, Double> instruments,
            double driftAnnual,
            double volatilityAnnual,
            double stepSeconds,
            double spreadBps,
            double paretoShape,
            long paretoScale) {

        private static final Map<String, Double> DEFAULT_INSTRUMENTS = Map.of(
                "RELIANCE", 2_500.00d,
                "TCS", 3_800.00d,
                "INFY", 1_500.00d,
                "WIPRO", 450.00d);

        public Model {
            instruments = instruments == null || instruments.isEmpty()
                    ? DEFAULT_INSTRUMENTS
                    : Map.copyOf(instruments);
            instruments.forEach((ticker, price) -> {
                if (price == null || price <= 0 || !Double.isFinite(price)) {
                    throw new IllegalArgumentException("opening price must be positive: " + ticker);
                }
            });
            if (volatilityAnnual < 0) {
                throw new IllegalArgumentException("volatility must not be negative");
            }
            if (stepSeconds <= 0) {
                throw new IllegalArgumentException("stepSeconds must be positive");
            }
            if (spreadBps < 0) {
                throw new IllegalArgumentException("spread must not be negative");
            }
            if (paretoShape <= 0) {
                throw new IllegalArgumentException("paretoShape must be positive");
            }
            if (paretoScale <= 0) {
                throw new IllegalArgumentException("paretoScale must be positive");
            }
        }

        static Model defaults() {
            return new Model(DEFAULT_INSTRUMENTS, 0.08d, 0.25d, 1.0d, 5.0d, 1.5d, 100L);
        }
    }
}
