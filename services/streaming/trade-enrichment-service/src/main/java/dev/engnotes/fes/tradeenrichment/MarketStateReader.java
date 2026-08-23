package dev.engnotes.fes.tradeenrichment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.common.cache.MarketCacheKeys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Reads the market state the projector wrote, and folds the rolling window for one trade.
 *
 * <p>This class never decides whether the state it returns is fresh enough to use. That is the
 * freshness policy's job (ADR-027 puts the policy in the reader, ADR-034 defines it), and separating
 * the two keeps the read testable without a policy and the policy testable without a Redis.
 *
 * <p><strong>The fold is bounded at both ends.</strong> Buckets outside
 * {@code [tradeBucket - WINDOW_SECONDS, tradeBucket]} are discarded. The lower bound is the
 * five-minute horizon. The upper bound is what makes the result deterministic: without it, replaying
 * an old trade against a warm window folds in ticks that arrived after the trade executed and
 * produces a different {@code vwap5Min} than the live run did.
 */
@Component
public class MarketStateReader {

    private final StringRedisTemplate redis;
    private final RedisScript<List> readMarketState;

    public MarketStateReader(StringRedisTemplate redis, RedisScript<List> readMarketState) {
        this.redis = redis;
        this.readMarketState = readMarketState;
    }

    /**
     * @return the state for this ticker, or empty when no tick has ever been projected for it
     */
    @SuppressWarnings("unchecked")
    public Optional<MarketSnapshot> read(String ticker, long tradeEventTimestampMillis) {
        List<List<String>> result = redis.execute(readMarketState,
                List.of(MarketCacheKeys.tickKey(ticker), MarketCacheKeys.windowKey(ticker)));

        if (result == null || result.size() != 2) {
            throw new IllegalStateException(
                    "read-market-state.lua must return two arrays, got " + result);
        }

        Map<String, String> tick = pairs(result.get(0));
        if (tick.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(fold(tick, pairs(result.get(1)), tradeEventTimestampMillis));
    }

    static MarketSnapshot fold(Map<String, String> tick,
                               Map<String, String> window,
                               long tradeEventTimestampMillis) {

        long upper = MarketCacheKeys.bucketFor(tradeEventTimestampMillis);
        long lower = upper - MarketCacheKeys.WINDOW_SECONDS;

        double priceVolume = 0.0;
        double volume = 0.0;
        for (Map.Entry<String, String> field : window.entrySet()) {
            int separator = field.getKey().lastIndexOf(':');
            if (separator < 0) {
                // lastOffset, and anything else the projector may add later. Parsing a Kafka offset
                // as a bucket would fold it into a price.
                continue;
            }
            long bucket;
            try {
                bucket = Long.parseLong(field.getKey().substring(0, separator));
            } catch (NumberFormatException e) {
                continue;
            }
            if (bucket < lower || bucket > upper) {
                continue;
            }
            switch (field.getKey().substring(separator + 1)) {
                case "pv" -> priceVolume += Double.parseDouble(field.getValue());
                case "v" -> volume += Double.parseDouble(field.getValue());
                default -> {
                }
            }
        }

        return new MarketSnapshot(
                Long.parseLong(tick.get("eventTimestamp")),
                Double.parseDouble(tick.get("bidPrice")),
                Double.parseDouble(tick.get("askPrice")),
                Double.parseDouble(tick.get("lastTradedPrice")),
                priceVolume,
                volume);
    }

    /** Callers must check {@code windowVolume} first: a zero here is the window_empty case. */
    static double vwap(MarketSnapshot snapshot) {
        return snapshot.windowPriceVolume() / snapshot.windowVolume();
    }

    private static Map<String, String> pairs(List<String> flat) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            map.put(flat.get(i), flat.get(i + 1));
        }
        return map;
    }
}
