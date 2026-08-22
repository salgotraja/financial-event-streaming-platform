-- Monotonic projection of one tick (ADR-032).
--
-- Returns 1 applied, 0 the stored entry already carries this timestamp, -1 the incoming tick is
-- older than what is stored. The two rejections are distinguished because they mean opposite
-- things: a duplicate is ordinary at-least-once redelivery, an older tick under live consumption
-- points at the producer.
--
-- Read and write in one script rather than from the client, because a client-side read-then-write
-- races a second consumer of the same partition during a rebalance, which is the window this exists
-- to close.

local stored = redis.call('HGET', KEYS[1], 'eventTimestamp')
local incoming = tonumber(ARGV[1])

if stored then
    local current = tonumber(stored)
    if incoming < current then
        return -1
    end
    if incoming == current then
        return 0
    end
end

redis.call('HSET', KEYS[1],
    'eventTimestamp', ARGV[1],
    'bidPrice', ARGV[2],
    'askPrice', ARGV[3],
    'lastTradedPrice', ARGV[4],
    'volume', ARGV[5],
    'producedAt', ARGV[6],
    'correlationId', ARGV[7])

return 1
