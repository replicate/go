function limit(now, tokens, last_fill_time, tokens_requested, rate, capacity)
    -- Add tokens accrued since the last fill
    local time_since_fill = now - last_fill_time
    local tokens_to_add = (time_since_fill / 1e6) * rate

    -- Never fill more than the floor of tokens
    tokens = math.floor(math.min(tokens + tokens_to_add, capacity))

    -- Get the time the last token would have been filled
    if tokens == capacity then
        -- Always keep the last fill time up to date if the bucket is full so we
        -- start penalizing immediately
        last_fill_time = now
    else
        -- Add the number of tokens * the time to fill one token to the fill time
        last_fill_time = last_fill_time + (math.floor(tokens_to_add) * (1e6/rate))
    end

    -- Grant as many (whole) tokens as we can and remove them from the bucket
    local tokens_granted = math.min(tokens, tokens_requested)
    tokens = tokens - tokens_granted

    -- Calculate the time until the bucket is refilled
    local time_to_full_bucket = math.ceil(((capacity - tokens) / rate) - ((now - last_fill_time) / 1e6))

    return tokens, tokens_granted, last_fill_time, time_to_full_bucket
end
