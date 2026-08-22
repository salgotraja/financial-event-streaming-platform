package dev.engnotes.fes.marketdatacacheprojector;

import java.util.List;

import dev.engnotes.fes.events.MarketDataTickEvent;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Writes the latest market state for one ticker, and only if the tick is newer than what is stored.
 *
 * <p>The source-event timestamp is stored beside the value because a reader applies its own
 * freshness policy; this class never decides what "too old" means for anyone. Entries do not expire:
 * a TTL would turn a stalled feed into a cache miss, and a miss reads as a cold cache rather than as
 * the fault it is (ADR-032).
 *
 * <p>The key space prefix is a security boundary, not only a naming convention. The projector's
 * Redis grant is scoped to {@code market:*}.
 */
@Component
public class MarketStateProjection {

    public static final String KEY_PREFIX = "market:tick:";

    private final StringRedisTemplate redis;
    private final RedisScript<Long> projectTick;

    public MarketStateProjection(StringRedisTemplate redis, RedisScript<Long> projectTick) {
        this.redis = redis;
        this.projectTick = projectTick;
    }

    public ProjectionOutcome project(MarketDataTickEvent tick) {
        String ticker = tick.getTicker().toString();
        Long result = redis.execute(projectTick, List.of(KEY_PREFIX + ticker),
                Long.toString(tick.getEventTimestamp().toEpochMilli()),
                Double.toString(tick.getBidPrice()),
                Double.toString(tick.getAskPrice()),
                Double.toString(tick.getLastTradedPrice()),
                Long.toString(tick.getVolume()),
                Long.toString(tick.getProducedAt().toEpochMilli()),
                tick.getCorrelationId().toString());
        return ProjectionOutcome.of(result);
    }
}
