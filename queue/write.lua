-- Write commands take the form
--
--   EVALSHA sha 1 key seconds streams n sid [sid ...] field value [field value ...]
--
-- - `key` is the base key for the queue, e.g. "prediction:input:abcd1234"
-- - `seconds` determines the expiry timeout for all keys that make up the
--   queue.
-- - `streams` is the number of streams the queue should have. In reality, the
--   queue may temporarily have more streams, if `streams` was previously larger
--   and the queue is in the process of resizing.
-- - `n` is the number of streams this write will consider. It must be less than
--   or equal to `streams`.
-- - `sid` are the stream IDs to consider writing to. They must be in the range
--   [0, `streams`). The message will be written to the shortest of the selected
--   streams.
--
-- Note: strictly, it is illegal for a script to manipulate keys that are not
-- explicitly passed to EVAL{,SHA}, but in practice this is fine as long as all
-- keys are on the same server (e.g. in cluster scenarios). In our case a single
-- queue, which may be composed of multiple streams and metadata keys, is always
-- on the same server.

local base = KEYS[1]
local ttl = tonumber(ARGV[1], 10)
local writestreams = tonumber(ARGV[2], 10)
local n = tonumber(ARGV[3], 10)
local sids = {unpack(ARGV, 4, 4 + n - 1)}
local fields = {unpack(ARGV, 4 + n, #ARGV)}

local key_meta = base .. ':meta'
local key_notifications = base .. ':notifications'

-- Check args
if writestreams < 1 then
  return redis.error_reply('ERR streams must be greater than or equal to 1')
end

if n < 1 then
  return redis.error_reply('ERR n must be greater than or equal to 1')
end

if n > writestreams then
  return redis.error_reply('ERR n may not be greater than streams')
end

for i = 1, n do
  if tonumber(sids[i]) < 0 or tonumber(sids[i]) >= writestreams then
    return redis.error_reply('ERR each sid must be in the range [0, streams)')
  end
end

-- How many streams are currently active?
local readstreams = tonumber(redis.call('HGET', key_meta, 'streams') or 1)

-- Check XLEN of all readstreams beyond writestreams and only update the value
-- in the meta key if all are empty.
local update = true

if readstreams > writestreams then
  for i = writestreams + 1, readstreams do
    local xlen = redis.call('XLEN', base .. ':s' .. i)
    if xlen > 0 then
      update = false
      break
    end
  end
end

-- Update streams in meta if writestreams > readstreams or if writestreams <
-- readstreams and all streams beyond writestreams are already empty.
if update and readstreams ~= writestreams then
  redis.call('HSET', key_meta, 'streams', writestreams)
end

-- Find the shortest stream
local selected_sid = sids[1]

if n > 1 then
  local len = -1
  for i = 1, n do
    local key = base .. ':s' .. sids[i]
    local xlen = redis.call('XLEN', key)

    -- It doesn't get shorter than empty
    if xlen == 0 then
      selected_sid = sids[i]
      break
    end

    -- If this is the first stream or the shortest so far, choose it.
    if len == -1 or xlen < len then
      len = xlen
      selected_sid = sids[i]
    end
  end
end

-- Add the message to the selected stream
local key_stream = base .. ':s' .. selected_sid
local id = redis.call('XADD', key_stream, '*', unpack(fields))

-- Add a notification to the notifications stream
redis.call('XADD', key_notifications, 'MAXLEN', '1', '*', 's', selected_sid)

-- Set expiry on all selected streams + meta key
redis.call('EXPIRE', key_stream, ttl)
redis.call('EXPIRE', key_meta, ttl)
redis.call('EXPIRE', key_notifications, ttl)

return id
