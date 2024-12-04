-- Lag commands take the form
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

  -- the group must exist for us to measure its lag. we create it here; if it
  -- already exists this returns a BUSYGROUP error which we ignore
  redis.pcall('XGROUP', 'CREATE', stream, group, '0', 'MKSTREAM')

  local info = redis.pcall('XINFO', 'GROUPS', stream)
  if info['err'] == 'ERR no such key' then
    -- if the stream doesn't exist, treat it as zero lag
  elseif info['err'] then
    return redis.error_reply(info['err']..' accessing '..stream)
  else
    for i,v in ipairs(info) do
      if v[2] == group then
        if not v[12] then
          -- lag can be nil; we propagate this to the caller
          return redis.error_reply('ERR unknown lag for group '..group..' on stream '..stream)
        end

        result = result + v[12]
        break
      end
    end
  end
end

return result
