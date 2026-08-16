package dev.engnotes.fes.referencedata;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic            destination topic. The workload identity for this service is authorised to
 *                         write this topic and nothing else, so changing it requires an IAM policy
 *                         change too.
 * @param producerIdentity the workload identity stamped onto every published record for FR-10.5.
 *                         Configured, never taken from the caller: a value the caller supplies is a
 *                         claim, not a provenance record.
 * @param seed             the synthetic instrument master (FR-10.1)
 */
@ConfigurationProperties(prefix = "fes.reference-data-service")
public record ReferenceDataProperties(
        @DefaultValue("reference-data.instruments") String topic,
        @DefaultValue("reference-data-service") String producerIdentity,
        Seed seed) {

    public ReferenceDataProperties {
        seed = seed == null ? Seed.defaults() : seed;
    }

    /**
     * @param enabled     publish the master on startup. Off by default so a restart does not
     *                    re-announce every instrument into a topic other services are consuming, and
     *                    so tests that assert on a single record are not sifting seed traffic.
     * @param instruments the instrument master. Static reference data, so it is configuration rather
     *                    than a generated stream.
     */
    public record Seed(boolean enabled, List<Instrument> instruments) {

        private static final List<Instrument> DEFAULT_INSTRUMENTS = List.of(
                new Instrument("INS-RELIANCE", "RELIANCE", "NSE", "INE002A01018", "EQUITY", "INR",
                        "ENERGY", 6_766_000_000L),
                new Instrument("INS-TCS", "TCS", "NSE", "INE467B01029", "EQUITY", "INR",
                        "INFORMATION_TECHNOLOGY", 3_620_000_000L),
                new Instrument("INS-INFY", "INFY", "NSE", "INE009A01021", "EQUITY", "INR",
                        "INFORMATION_TECHNOLOGY", 4_150_000_000L),
                new Instrument("INS-WIPRO", "WIPRO", "NSE", "INE075A01022", "EQUITY", "INR",
                        "INFORMATION_TECHNOLOGY", 10_460_000_000L));

        public Seed {
            instruments = instruments == null || instruments.isEmpty()
                    ? DEFAULT_INSTRUMENTS
                    : List.copyOf(instruments);
        }

        static Seed defaults() {
            return new Seed(false, DEFAULT_INSTRUMENTS);
        }
    }

    /**
     * One instrument master row. FR-10.2 asks for market capitalisation or the inputs to derive it;
     * {@code sharesOutstanding} with the price from {@code market-data.ticks} is those inputs, which
     * is why no capitalisation field is stored or published.
     */
    public record Instrument(
            String instrumentId,
            String ticker,
            String exchange,
            String isin,
            String securityType,
            String currency,
            String sector,
            long sharesOutstanding) {

        public Instrument {
            if (instrumentId == null || instrumentId.isBlank()) {
                throw new IllegalArgumentException("instrumentId is required");
            }
            if (ticker == null || ticker.isBlank()) {
                throw new IllegalArgumentException("ticker is required for " + instrumentId);
            }
            if (sharesOutstanding <= 0) {
                throw new IllegalArgumentException("sharesOutstanding must be positive for " + instrumentId);
            }
        }
    }
}
