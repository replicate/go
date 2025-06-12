require("limit")

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

function test_rate_less_than_1()
	-- This test just tests the core logic of the rate limiter

	-- Set the base time to some random time (this is the time I wrote this test)
	local now = 1749676283*1e6
	-- Make this a little bit in the past
	local time = tostring(now - 1e2)
	local state = {"1", tostring(now - 1e2), "0.4", "1"}

	-- If this is a new limiter, the bucket is full
	local tokens = tonumber(state[1], 10) or capacity
	
	-- NOTE: tonumber with base 10 will be sad here, unlike from redis
	local last_fill_time = tonumber(state[2]) or now

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 1, 0.4, 1)

	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 1, "Wrong number of tokens granted for base case")
	assert(time_to_full_bucket == 3, "Wrong time_to_fill_bucket value for base case")

	-- Advance the time by 1 second. The time to fill the bucket should say 2 seconds now
	print()
	now = now + 1e6

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 1, 0.4, 1)

	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 0, "Wrong number of tokens granted after 1 second")
	assert(time_to_full_bucket == 2, "Wrong time_to_fill_bucket value after 1 second")
	
	-- Advance the time by 1 second. The time to fill the bucket should say 1 seconds now
	print()
	now = now + 1e6

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 1, 0.4, 1)

	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 0, "Wrong number of tokens granted after 2 seconds")
	assert(time_to_full_bucket == 1, "Wrong time_to_fill_bucket value after 2 seconds")
	
	-- Advance the time by 0.5 seconds. The bucket should grant a token, and say 3 seconds left to fill
	print()
	now = now + 5e5

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 1, 0.4, 1)

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

	-- If this is a new limiter, the bucket is full
	local tokens = tonumber(state[1], 10) or capacity
	
	-- NOTE: tonumber with base 10 will be sad here, unlike from redis
	local last_fill_time = tonumber(state[2]) or now

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 20, 10, 100)

	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 20, "Wrong number of tokens granted for base case")
	assert(time_to_full_bucket == 2, "Wrong time_to_fill_bucket value for base case")

	-- Advance the time by 1 second. There should be 90 tokens in the bucket now
	print()
	now = now + 1e6

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 50, 10, 100)

	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 50, "Wrong number of tokens granted after 1 second")
	assert(time_to_full_bucket == 6, "Wrong time_to_fill_bucket value after 1 second")
	
	-- Advance the time by 1 second. There should be 50 tokens in the bucket now
	print()
	now = now + 1e6

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 60, 10, 100)

	print_return_val(tokens_granted, tokens, time_to_full_bucket)

	assert(tokens_granted == 50, "Wrong number of tokens granted after 2 seconds")
	assert(time_to_full_bucket == 10, "Wrong time_to_fill_bucket value after 2 seconds")
	
	-- Advance the time by 20 seconds. There should be 100 tokens in the bucket now
	print()
	now = now + 2e7

	tokens, tokens_granted, last_fill_time, time_to_full_bucket = limit(now, tokens, last_fill_time, 60, 10, 100)

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