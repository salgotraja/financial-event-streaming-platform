package dev.engnotes.fes.tradeenrichment;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.TradeEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Turns one {@link TradeEvent} into one {@link EnrichedTradeEvent}, or refuses to.
 *
 * <p>{@code EnrichedTradeEvent} has no optional fields, so every field must be computable or the
 * event must not be produced at all. That is why an unusable cache entry raises rather than
 * substituting a default: a zero {@code vwap5Min} would read downstream as a real price of zero.
 *
 * <p><strong>The freshness comparison is event-time on both sides.</strong>
 * {@code trade.eventTimestamp} minus the cached tick's {@code eventTimestamp}, never a wall clock.
 * A wall-clock age would give the same record a different value on every replay and could flip it
 * between enriched and dead-lettered, which would make the enriched stream a recording of when the
 * service happened to run rather than a projection of the trade stream (ADR-034).
 *
 * <p>The clock is used for {@code enrichedAt} and {@code enrichmentLatencyMs} only. Those two are
 * processing telemetry and are expected to differ between a live run and a replay.
 */
@Component
public class TradeEnricher {

    private static final double CRORE = 1e7;

    private final MarketStateReader reader;
    private final InstrumentCache instruments;
    private final Duration maxAge;
    private final Clock clock;

    // Explicit, because a second (package-private, test-only) constructor with the same arity
    // means Spring cannot infer which one to autowire and falls back to a default constructor that
    // does not exist, failing every real ApplicationContext this service starts.
    @Autowired
    public TradeEnricher(MarketStateReader reader,
                         InstrumentCache instruments,
                         EnrichmentProperties properties,
                         Clock clock) {
        this(reader, instruments, properties.marketDataMaxAge(), clock);
    }

    TradeEnricher(MarketStateReader reader, InstrumentCache instruments, Duration maxAge, Clock clock) {
        this.reader = reader;
        this.instruments = instruments;
        this.maxAge = maxAge;
        this.clock = clock;
    }

    public EnrichedTradeEvent enrich(TradeEvent trade, long consumeStartMillis) {
        validate(trade);

        String ticker = trade.getTicker().toString();
        long tradeAt = trade.getEventTimestamp().toEpochMilli();

        MarketSnapshot snapshot = reader.read(ticker, tradeAt)
                .orElseThrow(() -> unavailable(UnavailableReason.TICK_ABSENT,
                        "no market state has been projected for " + ticker));

        long age = tradeAt - snapshot.eventTimestampMillis();
        if (age < 0) {
            throw unavailable(UnavailableReason.FUTURE,
                    "the cached tick for " + ticker + " postdates the trade by " + (-age) + "ms");
        }
        if (age > maxAge.toMillis()) {
            throw unavailable(UnavailableReason.STALE,
                    "the cached tick for " + ticker + " is " + age + "ms old, over the " + maxAge + " limit");
        }
        if (snapshot.windowVolume() <= 0.0) {
            throw unavailable(UnavailableReason.WINDOW_EMPTY,
                    "the rolling window for " + ticker + " carries no volume in this trade's horizon");
        }

        Instrument instrument = instruments.find(ticker)
                .orElseThrow(() -> unavailable(UnavailableReason.INSTRUMENT_MISSING,
                        "the instrument master does not carry " + ticker));

        double mid = (snapshot.bidPrice() + snapshot.askPrice()) / 2.0;
        if (!Double.isFinite(mid) || mid <= 0.0) {
            // priceDeviation divides by this and is a non-optional double, so an Infinity here would
            // reach the topic rather than fail here.
            throw new IllegalArgumentException(
                    "The cached quote for " + ticker + " yields an unusable mid-price of " + mid);
        }

        Instant enrichedAt = clock.instant();
        return EnrichedTradeEvent.newBuilder()
                .setTrade(trade)
                .setMidPriceAtExecution(mid)
                .setSpreadAtExecution(snapshot.askPrice() - snapshot.bidPrice())
                .setVwap5Min(MarketStateReader.vwap(snapshot))
                .setMarketCap(instrument.sharesOutstanding() * snapshot.lastTradedPrice() / CRORE)
                .setPriceDeviation((trade.getPrice() - mid) / mid * 100.0)
                .setEnrichedAt(enrichedAt)
                .setEnrichmentLatencyMs(enrichedAt.toEpochMilli() - consumeStartMillis)
                .setMarketDataAgeMs(age)
                .build();
    }

    private static void validate(TradeEvent trade) {
        if (!Double.isFinite(trade.getPrice())) {
            throw new IllegalArgumentException(
                    "Trade " + trade.getTradeId() + " carries a non-finite price");
        }
        if (trade.getQuantity() < 0) {
            throw new IllegalArgumentException(
                    "Trade " + trade.getTradeId() + " carries a negative quantity");
        }
        if (trade.getTicker() == null || trade.getTicker().toString().isBlank()) {
            throw new IllegalArgumentException(
                    "Trade " + trade.getTradeId() + " carries a blank ticker");
        }
    }

    private static ReferenceDataUnavailableException unavailable(UnavailableReason reason, String detail) {
        return new ReferenceDataUnavailableException(reason, detail);
    }
}
