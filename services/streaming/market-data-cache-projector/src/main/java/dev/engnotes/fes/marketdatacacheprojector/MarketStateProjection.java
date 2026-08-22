package dev.engnotes.fes.marketdatacacheprojector;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import dev.engnotes.fes.events.MarketDataTickEvent;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Writes the latest market state for one ticker and that tick's contribution to the rolling window.
 *
 * <p>The source-event timestamp is stored beside the value because a reader applies its own
 * freshness policy; this class never decides what "too old" means for anyone. The latest-price entry
 * does not expire: a TTL would turn a stalled feed into a cache miss, and a miss reads as a cold
 * cache rather than as the fault it is (ADR-032). The window does expire, and the asymmetry is only
 * partly the reason it looks: a window holding nothing in the last five minutes is genuinely empty,
 * but a window that idled past the 600s TTL and is now repopulating is partial, not empty, and a
 * reader cannot tell the two apart from the hash alone (ADR-033).
 *
 * <p>Both keys carry the same hash tag, the braced ticker, so a clustered Redis keeps them in one
 * slot and the single script may write both. Without it the script would fail with CROSSSLOT there
 * while passing every local test against a standalone Redis.
 *
 * <p>The key space prefix is a security boundary, not only a naming convention. The projector's
 * Redis grant is scoped to {@code market:*}.
 *
 * <p>A tick with a non-finite price or a negative volume is rejected before the script runs. The
 * script's tick-hash write and its window write are separate Redis calls inside one script, and Redis
 * does not roll back a script that errors part-way; a NaN or Infinity reaching {@code
 * lastTradedPrice * volume} inside the script would abort after the tick hash had already landed,
 * corrupting a key with no TTL to age it out.
 *
 * <p>A tick with an event timestamp more than one hour ahead of this clock is rejected for a related
 * reason: the window's prune cutoff is derived from the incoming tick's own bucket, so one skewed
 * far-future timestamp would prune every live bucket in a single call. The clock is used only for
 * this bound; bucket assignment and pruning stay purely event-time, which is what keeps replay
 * deterministic.
 */
@Component
public class MarketStateProjection {

    static final int BUCKET_SECONDS = 10;
    static final int WINDOW_SECONDS = 300;
    static final int WINDOW_TTL_SECONDS = 600;
    static final Duration MAX_FUTURE_SKEW = Duration.ofHours(1);

    private final StringRedisTemplate redis;
    private final RedisScript<List> projectTick;
    private final Clock clock;

    public MarketStateProjection(StringRedisTemplate redis, RedisScript<List> projectTick, Clock clock) {
        this.redis = redis;
        this.projectTick = projectTick;
        this.clock = clock;
    }

    public static String tickKey(String ticker) {
        return "market:{" + ticker + "}:tick";
    }

    public static String windowKey(String ticker) {
        return "market:{" + ticker + "}:window";
    }

    @SuppressWarnings("unchecked")
    public ProjectionResult project(MarketDataTickEvent tick, long offset) {
        validate(tick);
        String ticker = tick.getTicker().toString();
        List<Long> result = redis.execute(projectTick,
                List.of(tickKey(ticker), windowKey(ticker)),
                Long.toString(tick.getEventTimestamp().toEpochMilli()),
                Double.toString(tick.getBidPrice()),
                Double.toString(tick.getAskPrice()),
                Double.toString(tick.getLastTradedPrice()),
                Long.toString(tick.getVolume()),
                Long.toString(tick.getProducedAt().toEpochMilli()),
                tick.getCorrelationId().toString(),
                Long.toString(offset),
                Integer.toString(BUCKET_SECONDS),
                Integer.toString(WINDOW_SECONDS),
                Integer.toString(WINDOW_TTL_SECONDS));
        return ProjectionResult.of(result);
    }

    private void validate(MarketDataTickEvent tick) {
        if (!Double.isFinite(tick.getBidPrice())
                || !Double.isFinite(tick.getAskPrice())
                || !Double.isFinite(tick.getLastTradedPrice())) {
            throw new IllegalArgumentException(
                    "Tick for " + tick.getTicker() + " carries a non-finite price");
        }
        if (tick.getVolume() < 0) {
            throw new IllegalArgumentException(
                    "Tick for " + tick.getTicker() + " carries a negative volume");
        }
        if (tick.getEventTimestamp().isAfter(clock.instant().plus(MAX_FUTURE_SKEW))) {
            throw new IllegalArgumentException(
                    "Tick for " + tick.getTicker() + " has an event timestamp more than "
                            + MAX_FUTURE_SKEW + " ahead of the clock");
        }
    }
}
