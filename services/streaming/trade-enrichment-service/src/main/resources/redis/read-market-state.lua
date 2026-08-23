-- One consistent read of the market state for a ticker (ADR-034).
--
-- KEYS[1] latest-price hash, KEYS[2] window hash. Both carry the same hash tag, so a clustered Redis
-- keeps them in one slot and this script may read both. Two separate calls could straddle a projector
-- write and pair a mid-price from tick N with a window that already includes tick N+1.
--
-- Read-only, and that is enforced outside this file: the trade-enrichment-service Redis user holds no
-- write command at all, so an edit adding one fails inside EVAL rather than silently succeeding.
--
-- Returns {tickFields, windowFields}, each a flat HGETALL array, empty when the key is absent. The
-- horizon filter is applied by the caller, not here, because it depends on the trade's own event
-- timestamp and keeping it in Java keeps it unit-testable without a Redis.

return {redis.call('HGETALL', KEYS[1]), redis.call('HGETALL', KEYS[2])}
