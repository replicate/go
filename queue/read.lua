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

-- LEGACY: Check if a stream exists at the old name. If it does, it will be
-- checked alongside all other streams.
--
-- This can be removed once everything is writing to new stream names.
local check_default_stream = redis.call('XLEN', base) > 0

-- Loop over streams to find a message
local function hasprefix(str, prefix)
   return string.sub(str, 1, string.len(prefix)) == prefix
end

local function checkstream (stream)
  local grp
  -- TODO: Remove this once we've migrated to using the new group everywhere.
  -- This allows us to stop using the stream name as the consumer group name,
  -- because new streams (`:sN`) will use the provided group name, while the old
  -- stream will just use its own name as the group, which is the current
  -- behavior.
  if stream == base then
    grp = base
  else
    grp = group
  end
  local reply = redis.pcall('XREADGROUP', 'GROUP', grp, consumer, 'COUNT', 1, 'STREAMS', stream, '>')
  -- false means a null reply from XREADGROUP, which means the stream is empty
  if not reply then
    return reply
  end

  if reply.err == nil then
    return reply
  end

  if hasprefix(reply['err'], 'NOGROUP No such key') then
    redis.call('XGROUP', 'CREATE', stream, grp, '0', 'MKSTREAM')
    redis.call('EXPIRE', stream, ttl)
    -- and try again, just once
    return redis.pcall('XREADGROUP', 'GROUP', grp, consumer, 'COUNT', 1, 'STREAMS', stream, '>')
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

for idx = 0, streams do
  local streamid = (offset + idx) % streams

  -- LEGACY: for now, if we're checking stream 0, also check the default stream.
  if streamid == 0 and check_default_stream then
    local reply = checkstream(base)
    if reply then
      redis.call('HSET', key_meta, 'offset', (offset + idx + 1) % streams)
      return reply
    end
  end

  local reply = checkstream(base .. ':s' .. streamid)
  if reply then
    redis.call('HSET', key_meta, 'offset', (offset + idx + 1) % streams)
    return reply
  end
end

-- We fell off the end of the loop without finding a message.
return false
