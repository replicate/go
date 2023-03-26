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

-- If rate and capacity exist in the key, the arguments are ignored. This allows
-- for workflows where custom limits exist for users but are not known until
-- after the limiter has run.
local rate = tonumber(state[3], 10) or tonumber(ARGV[2], 10) or default_rate
local capacity = tonumber(state[4], 10) or tonumber(ARGV[3], 10) or default_capacity

-- If this is a new limiter, the bucket is full
local tokens = tonumber(state[1], 10) or capacity
local last_fill_time = tonumber(state[2], 10) or now

-- Add tokens accrued since the last fill
local time_since_fill = now - last_fill_time
local tokens_to_add = (time_since_fill / 1e6) * rate
tokens = math.min(tokens + tokens_to_add, capacity)

-- Grant as many (whole) tokens as we can and remove them from the bucket
local tokens_granted = math.min(math.floor(tokens), tokens_requested)
tokens = tokens - tokens_granted

-- Calculate the time until the bucket is refilled
local time_to_full_bucket = math.ceil((capacity - tokens) / rate)

-- Expire the key one second after the bucket is full
redis.call('EXPIRE', KEYS[1], time_to_full_bucket + 1)

-- Save state and return the results
redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_fill_time', now, 'rate', rate, 'capacity', capacity)
return {tokens_granted, math.floor(tokens), time_to_full_bucket}
