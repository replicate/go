-- pendingcount commands take the form
--
--   EVALSHA sha 1 key group
--
-- Note: strictly, it is illegal for a script to manipulate keys that are not
-- explicitly passed to EVAL{,SHA}, but in practice this is fine as long as all
-- keys are on the same server (e.g. in cluster scenarios). In our case a single
-- queue, which may be composed of multiple streams and metadata keys, is always
-- on the same server.

local base = KEYS[1]
local group = ARGV[1]

local key_meta = base .. ':meta'

local streams = tonumber(redis.call('HGET', key_meta, 'streams') or 1)
local result = 0

for idx = 0, streams-1 do
  local stream = base .. ':s' .. idx

  local info = redis.pcall('XPENDING', stream, group)
  if info['err'] then
    if string.match(info['err'], '^NOGROUP ') then
      -- if either the stream or group don't exist, there are zero pending entries
    else
      return redis.error_reply(info['err']..' accessing '..stream)
    end
  else
    result = result + info[1]
  end
end

return result
