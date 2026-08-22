package dev.engnotes.fes.marketdatacacheprojector;

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
 * cache rather than as the fault it is (ADR-032). The window does expire, and the asymmetry is
 * deliberate: a window holding nothing in the last five minutes is genuinely empty, so an expiry
 * there says something true (ADR-033).
 *
 * <p>Both keys carry the same hash tag, the braced ticker, so a clustered Redis keeps them in one
 * slot and the single script may write both. Without it the script would fail with CROSSSLOT there
 * while passing every local test against a standalone Redis.
 *
 * <p>The key space prefix is a security boundary, not only a naming convention. The projector's
 * Redis grant is scoped to {@code market:*}.
 */
@Component
public class MarketStateProjection {

    static final int BUCKET_SECONDS = 10;
    static final int WINDOW_SECONDS = 300;
    static final int WINDOW_TTL_SECONDS = 600;

    private final StringRedisTemplate redis;
    private final RedisScript<List> projectTick;

    public MarketStateProjection(StringRedisTemplate redis, RedisScript<List> projectTick) {
        this.redis = redis;
        this.projectTick = projectTick;
    }

    public static String tickKey(String ticker) {
        return "market:{" + ticker + "}:tick";
    }

    public static String windowKey(String ticker) {
        return "market:{" + ticker + "}:window";
    }

    @SuppressWarnings("unchecked")
    public ProjectionResult project(MarketDataTickEvent tick, long offset) {
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
}
