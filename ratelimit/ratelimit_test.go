package ratelimit

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/go/test"
)

func TestLimiterIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	limiter, _ := NewLimiter(rdb)
	require.NoError(t, limiter.Prepare(ctx))

	// result counters
	permitted := 0
	denied := 0

	// test parameters
	duration := 10
	demandRate := 200

	// rate limiter parameters
	rate := 42.0
	capacity := 5

	deadline := time.After(time.Duration(duration) * time.Second)
	ticker := time.NewTicker(time.Second / time.Duration(demandRate))

Outer:
	for {
		select {
		case <-ticker.C:
			r, err := limiter.Take(ctx, "limit:testkey", 1, rate, capacity)
			require.NoError(t, err)
			if r.OK {
				permitted++
			} else {
				denied++
			}
		case <-deadline:
			break Outer
		case <-ctx.Done():
			return
		}
	}

	expectedTotal := demandRate * duration
	expectedPermitted := int(math.Floor(rate * float64(duration)))

	// allow up to 1% error
	assert.InDelta(t, expectedTotal, permitted+denied, float64(expectedTotal/100))
	assert.InDelta(t, expectedPermitted, permitted, float64(expectedPermitted/100))
}

// Regression test for a bug where we weren't setting a TTL on the key the first
// time the limiter was called.
func TestLimiterAlwaysSetsExpiry(t *testing.T) {

	key := fmt.Sprintf("limit:testkey:%d", rand.Uint32())

	mr, rdb := test.MiniRedis(t)
	ctx := test.Context(t)
	limiter, _ := NewLimiter(rdb)
	require.NoError(t, limiter.Prepare(ctx))

	// Clean up at the end of the test
	t.Cleanup(func() { rdb.Del(ctx, key) })

	_, _ = limiter.Take(ctx, key, 1, 100, 10000)

	mr.FastForward(time.Minute)
	assert.False(t, mr.Exists(key))
}

func TestLimiterTakeWithNegativeInputsReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := redis.NewClient(&redis.Options{})
	limiter, _ := NewLimiter(client)
	{
		_, err := limiter.Take(ctx, "testkey", -1, 1, 1)
		require.ErrorIs(t, err, ErrNegativeInput)
	}
	{
		_, err := limiter.Take(ctx, "testkey", 1, -1, 1)
		require.ErrorIs(t, err, ErrNegativeInput)
	}
	{
		_, err := limiter.Take(ctx, "testkey", 1, 1, -1)
		require.ErrorIs(t, err, ErrNegativeInput)
	}
}

func TestLimiterSetOptionsWithNegativeInputsReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := redis.NewClient(&redis.Options{})
	limiter, _ := NewLimiter(client)
	{
		err := limiter.SetOptions(ctx, "testkey", -1, 1)
		require.ErrorIs(t, err, ErrNegativeInput)
	}
	{
		err := limiter.SetOptions(ctx, "testkey", 1, -1)
		require.ErrorIs(t, err, ErrNegativeInput)
	}
}
