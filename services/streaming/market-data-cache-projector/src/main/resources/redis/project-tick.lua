-- Monotonic projection of one tick, and its contribution to the rolling window (ADR-032, ADR-033).
--
-- KEYS[1] latest-price hash, KEYS[2] window hash. Both carry the same hash tag, so a clustered Redis
-- keeps them in one slot and this script may write both atomically. They must never disagree.
--
-- Two guards, deliberately different. The latest-price hash compares the source-event timestamp:
-- strictly newer applies, equal is a duplicate, older is rejected. The window compares the Kafka
-- offset instead, because two distinct ticks can share a millisecond and the timestamp guard would
-- drop the second one's volume as though it were a duplicate, when that volume was really traded.
--
-- The offset guard has two limits. It relies on every tick for a ticker landing on one partition,
-- which holds because the producer keys on ticker; a change in the topic's partition count breaks
-- the comparability of stored offsets. And a deliberate rebuild must delete the window key first,
-- or every replayed record is skipped as already applied.
--
-- Buckets are assigned and pruned by the incoming tick's own eventTimestamp, never by a wall clock,
-- so replaying the topic rebuilds identical state.
--
-- Returns {tickOutcome, windowApplied, windowBuckets}: tickOutcome 1 applied, 0 duplicate,
-- -1 older; windowApplied 1 or 0; windowBuckets the number of buckets the window now holds.

local tickKey = KEYS[1]
local windowKey = KEYS[2]

local eventTimestamp = tonumber(ARGV[1])
local lastTradedPrice = tonumber(ARGV[4])
local volume = tonumber(ARGV[5])
local offset = tonumber(ARGV[8])
local bucketSeconds = tonumber(ARGV[9])
local windowSeconds = tonumber(ARGV[10])
local windowTtlSeconds = tonumber(ARGV[11])

local outcome = 1
local stored = redis.call('HGET', tickKey, 'eventTimestamp')
if stored then
    local current = tonumber(stored)
    if eventTimestamp < current then
        outcome = -1
    elseif eventTimestamp == current then
        outcome = 0
    end
end

if outcome == 1 then
    redis.call('HSET', tickKey,
        'eventTimestamp', ARGV[1],
        'bidPrice', ARGV[2],
        'askPrice', ARGV[3],
        'lastTradedPrice', ARGV[4],
        'volume', ARGV[5],
        'producedAt', ARGV[6],
        'correlationId', ARGV[7])
end

local windowApplied = 0
local lastOffset = redis.call('HGET', windowKey, 'lastOffset')
if (not lastOffset) or offset > tonumber(lastOffset) then
    -- string.format rather than concatenation: Lua numbers are doubles here, and tostring on a
    -- large one can render in exponent form, which would produce a field name nothing matches.
    local bucket = math.floor(eventTimestamp / 1000 / bucketSeconds) * bucketSeconds
    local bucketName = string.format('%d', bucket)

    redis.call('HINCRBYFLOAT', windowKey, bucketName .. ':pv', lastTradedPrice * volume)
    redis.call('HINCRBYFLOAT', windowKey, bucketName .. ':v', volume)
    redis.call('HSET', windowKey, 'lastOffset', ARGV[8])

    local cutoff = bucket - windowSeconds
    local fields = redis.call('HKEYS', windowKey)
    for i = 1, #fields do
        local second = string.match(fields[i], '^(%d+):')
        if second and tonumber(second) < cutoff then
            redis.call('HDEL', windowKey, fields[i])
        end
    end

    redis.call('EXPIRE', windowKey, windowTtlSeconds)
    windowApplied = 1
end

local buckets = 0
local remaining = redis.call('HKEYS', windowKey)
for i = 1, #remaining do
    if string.match(remaining[i], ':pv$') then
        buckets = buckets + 1
    end
end

return {outcome, windowApplied, buckets}
