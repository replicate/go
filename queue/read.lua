-- Read commands take the form
--
--   EVALSHA sha 1 key seconds group consumer
--
-- - `key` is the base key for the queue, e.g. "prediction:input:abcd1234".
-- - `seconds` determines the expiry timeout for all keys that make up the
--   queue.
-- - `group` is the name of the consumer group associated to the underlying
--    streams.
-- - `consumer` is the name of the consumer within the group.
--
-- Note: strictly, it is illegal for a script to manipulate keys that are not
-- explicitly passed to EVAL{,SHA}, but in practice this is fine as long as all
-- keys are on the same server (e.g. in cluster scenarios). In our case a single
-- queue, which may be composed of multiple streams and metadata keys, is always
-- on the same server.

local base = KEYS[1]
local ttl = tonumber(ARGV[1], 10)
local group = ARGV[2]
local consumer = ARGV[3]

local key_meta = base .. ':meta'

-- How many streams are available to read?
local streams = tonumber(redis.call('HGET', key_meta, 'streams') or 1)

-- Loop over streams to find a message
local function hasprefix(str, prefix)
   return string.sub(str, 1, string.len(prefix)) == prefix
end

local function checkstream (stream)
  local reply = redis.pcall('XREADGROUP', 'GROUP', group, consumer, 'COUNT', 1, 'STREAMS', stream, '>')
  -- false means a null reply from XREADGROUP, which means the stream is empty
  if not reply then
    return reply
  end

  if reply.err == nil then
    return reply
  end

  if hasprefix(reply['err'], 'NOGROUP No such key') then
    redis.call('XGROUP', 'CREATE', stream, group, '0', 'MKSTREAM')
    redis.call('EXPIRE', stream, ttl)
    -- and try again, just once
    return redis.pcall('XREADGROUP', 'GROUP', group, consumer, 'COUNT', 1, 'STREAMS', stream, '>')
  end

  return reply
end

-- Use a global shared offset to determine where to start reading. Whenever we
-- find a message, update the shared offset to point to the *next* stream. This
-- should ensure fairness of reads across all the streams.
--
-- It doesn't matter if offset is >= streams, because we ensure that the value
-- is appropriately wrapped before using it.
local offset = tonumber(redis.call('HGET', key_meta, 'offset') or 0)

for idx = 0, streams-1 do
  local streamid = (offset + idx) % streams

  local reply = checkstream(base .. ':s' .. streamid)
  if reply then
    redis.call('HSET', key_meta, 'offset', (offset + idx + 1) % streams)
    redis.call('EXPIRE', key_meta, ttl)
    return reply
  end
end

-- We fell off the end of the loop without finding a message.
return false
