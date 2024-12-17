-- Len commands take the form
--
--   EVALSHA sha 1 key
--
-- Note: strictly, it is illegal for a script to manipulate keys that are not
-- explicitly passed to EVAL{,SHA}, but in practice this is fine as long as all
-- keys are on the same server (e.g. in cluster scenarios). In our case a single
-- queue, which may be composed of multiple streams and metadata keys, is always
-- on the same server.

-- Note: This script performs an additional step in logic to calculate how many elements
-- are in the queue that are unclaimed. This is done by calculating the number of elements
-- and subtracting the result from XPENDING. XPENDING requires a group name to be passed, so
-- the script will iterate through all groups in the stream.

local base = KEYS[1]

local key_meta = base .. ':meta'

local streams = tonumber(redis.call('HGET', key_meta, 'streams') or 1)
local result = 0

for idx = 0, streams-1 do
  local success, groupInfo = pcall(function()
      return redis.call('XINFO', 'GROUPS', base .. ':s' .. idx)
  end)
  local pendingCount = 0
  if success and groupInfo ~= nil and #groupInfo ~= 0 then
    for _, group in ipairs(groupInfo) do
      local groupName = group[2]
      local pendingInfo = redis.call('XPENDING', base .. ':s' .. idx, groupName)
      pendingCount = pendingCount + pendingInfo[1]
    end
  end

  result = result + redis.call('XLEN', base .. ':s' .. idx) - pendingCount
  result = (result <= 0) and nil or result
end

return result
