-- If the key doesn't exist and the rate + capacity arguments are not provided,
-- the default rate limit is 50 req/s with a burst capacity of one minute's
-- worth of requests, i.e. 50 * 60 = 3000.
local default_rate = 50
local default_capacity = 3000

-- Load current state and time
local state = redis.call('HMGET', KEYS[1], 'tokens', 'last_fill_time', 'rate', 'capacity')
local time = redis.call('TIME')
local now = tonumber(time[1], 10) * 1e6 + tonumber(time[2], 10)

-- Process arguments. All are optional.
local tokens_requested = tonumber(ARGV[1], 10) or 1
local rate = tonumber(ARGV[2], 10) or tonumber(state[3], 10) or default_rate
local capacity = tonumber(ARGV[3], 10) or tonumber(state[4], 10) or default_capacity

-- If this is a new limiter, the bucket is full
local tokens = tonumber(state[1], 10) or capacity
local last_fill_time = tonumber(state[2], 10) or now

tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, tokens_requested, rate, capacity)

-- Save state and return the results
redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_fill_time', last_fill_time, 'rate', rate, 'capacity', capacity)

-- Expire the key one second after the bucket is full
redis.call('EXPIRE', KEYS[1], time_to_full_bucket + 1)

return {tokens_granted, math.floor(tokens), time_to_full_bucket}