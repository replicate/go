-- Len commands take the form
--
--   EVALSHA sha 1 key
--
-- Note: strictly, it is illegal for a script to manipulate keys that are not
-- explicitly passed to EVAL{,SHA}, but in practice this is fine as long as all
-- keys are on the same server (e.g. in cluster scenarios). In our case a single
-- queue, which may be composed of multiple streams and metadata keys, is always
-- on the same server.

local base = KEYS[1]

local key_meta = base .. ':meta'

local streams = tonumber(redis.call('HGET', key_meta, 'streams') or 1)
local result = 0

-- LEGACY: Include the base stream. This can be removed once everything is using
-- new stream names.
result = result + redis.call('XLEN', base)

for idx = 0, streams do
  result = result + redis.call('XLEN', base .. ':s' .. idx)
end

return result
