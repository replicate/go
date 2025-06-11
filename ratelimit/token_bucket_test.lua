print('-- TEST SUITE BEGIN --')

function print_table(t)
	for k, v in pairs(t) do
		print(k.."="..v)
	end
end

function print_list(t)
	for k, v in pairs(t) do
		print(v)
	end
end

-- Left in as a utility function for debugging. If you need to see the state, use this
function print_state(s)
	print("tokens="..s[1].." last_fill_time="..s[2].." rate="..s[3].." capacity="..s[4])
end

function print_return_val(granted, tokens, time_to_full_bucket)
	print("tokens_granted="..granted.." tokens_in_bucket="..tokens.." time_to_full_bucket="..time_to_full_bucket)
end

-- This function should just hold the core logic of token_bucket.lua, ignoring any redis
function limit(now, state, tokens_requested, rate, capacity)
	-- If this is a new limiter, the bucket is full
	local tokens = tonumber(state[1], 10) or capacity
	-- NOTE: tonumber with base 10 will be sad here, unlike from redis
	local last_fill_time = tonumber(state[2]) or now
	
	-- Add tokens accrued since the last fill
	local time_since_fill = now - last_fill_time
	local tokens_to_add = (time_since_fill / 1e6) * rate

	-- Get the time the last token would have been filled
	if tokens == capacity then
		-- Always keep the last fill time up to date if the bucket is full so we
		-- start penalizing immediately
		last_fill_time = now
	else
		-- Add the number of tokens * the time to fill one token to the fill time
		last_fill_time = last_fill_time + (math.floor(tokens_to_add) * (1e6/rate))
	end

	-- Never fill more than the floor of tokens
	tokens = math.floor(math.min(tokens + tokens_to_add, capacity))
	
	-- Grant as many (whole) tokens as we can and remove them from the bucket
	local tokens_granted = math.min(tokens, tokens_requested)
	tokens = tokens - tokens_granted
	
	-- Calculate the time until the bucket is refilled
	local time_to_full_bucket = math.ceil(((capacity - tokens) / rate) - ((now - last_fill_time) / 1e6))

	return tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity
end

function test_rate_less_than_1()
	-- This test just tests the core logic of the rate limiter

	-- Set the base time to some random time (this is the time I wrote this test)
	local now = 1749676283*1e6
	-- Make this a little bit in the past
	local time = tostring(now - 1e2)
	local state = {"1", tostring(now - 1e2), "0.4", "1"}

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 1, 0.4, 1)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 1, "Wrong number of tokens granted for base case")
	assert(time_to_full_bucket == 3, "Wrong time_to_fill_bucket value for base case")

	-- Advance the time by 1 second. The time to fill the bucket should say 2 seconds now
	print()
	now = now + 1e6

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 1, 0.4, 1)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 0, "Wrong number of tokens granted after 1 second")
	assert(time_to_full_bucket == 2, "Wrong time_to_fill_bucket value after 1 second")
	
	-- Advance the time by 1 second. The time to fill the bucket should say 1 seconds now
	print()
	now = now + 1e6

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 1, 0.4, 1)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 0, "Wrong number of tokens granted after 2 seconds")
	assert(time_to_full_bucket == 1, "Wrong time_to_fill_bucket value after 2 seconds")
	
	-- Advance the time by 0.5 seconds. The bucket should grant a token, and say 3 seconds left to fill
	print()
	now = now + 5e5

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 1, 0.4, 1)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 1, "Wrong number of tokens granted after 3 seconds")
	assert(time_to_full_bucket == 3, "Wrong time_to_fill_bucket value after 3 seconds")
end

function test_rate_greater_than_1()
	-- This test just tests the core logic of the rate limiter

	-- Set the base time to some random time (this is the time I wrote this test)
	local now = 1749676283*1e6
	-- Make this a little bit in the past
	local time = tostring(now - 1e2)
	local state = {"100", tostring(now - 1e2), "10", "100"}

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 20, 10, 100)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 20, "Wrong number of tokens granted for base case")
	assert(time_to_full_bucket == 2, "Wrong time_to_fill_bucket value for base case")

	-- Advance the time by 1 second. There should be 90 tokens in the bucket now
	print()
	now = now + 1e6

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 50, 10, 100)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 50, "Wrong number of tokens granted after 1 second")
	assert(time_to_full_bucket == 6, "Wrong time_to_fill_bucket value after 1 second")
	
	-- Advance the time by 1 second. There should be 50 tokens in the bucket now
	print()
	now = now + 1e6

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 60, 10, 100)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 50, "Wrong number of tokens granted after 2 seconds")
	assert(time_to_full_bucket == 10, "Wrong time_to_fill_bucket value after 2 seconds")
	
	-- Advance the time by 20 seconds. There should be 100 tokens in the bucket now
	print()
	now = now + 2e7

	tokens_granted, time_to_full_bucket, tokens, last_fill_time, rate, capacity = limit(now, state, 60, 10, 100)

	state = {tostring(tokens), tostring(last_fill_time), tostring(rate), tostring(capacity)}
	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 60, "Wrong number of tokens granted after 3 seconds")
	assert(time_to_full_bucket == 6, "Wrong time_to_fill_bucket value after 3 seconds")
end

print('-- TEST BEGIN: rate < 1 --')
test_rate_less_than_1()
print('-- TEST END --')

print('-- TEST BEGIN: rate > 1 --')
test_rate_greater_than_1()
print('-- TEST END --')

print('-- TEST SUITE END --')