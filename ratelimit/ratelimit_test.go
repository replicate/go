package ratelimit

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimiterIntegration(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts, err := redis.ParseURL(redisURL)
	require.NoError(t, err)

	client := redis.NewClient(opts)
	limiter, _ := NewLimiter(client)
	require.NoError(t, limiter.Prepare(ctx))

	// result counters
	permitted := 0
	denied := 0

	// test parameters
	duration := 10
	demandRate := 200

	// rate limiter parameters
	rate := 42
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
	expectedPermitted := rate * duration

	// allow up to 1% error
	assert.InDelta(t, expectedTotal, permitted+denied, float64(expectedTotal/100))
	assert.InDelta(t, expectedPermitted, permitted, float64(expectedPermitted/100))
}

// Regression test for a bug where we weren't setting a TTL on the key the first
// time the limiter was called.
func TestLimiterAlwaysSetsExpiry(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not set")
	}

	key := fmt.Sprintf("limit:testkey:%d", rand.Uint32())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts, err := redis.ParseURL(redisURL)
	require.NoError(t, err)

	client := redis.NewClient(opts)
	limiter, _ := NewLimiter(client)
	require.NoError(t, limiter.Prepare(ctx))

	// Clean up at the end of the test
	defer client.Del(ctx, key)

	_, _ = limiter.Take(ctx, key, 1, 100, 10000)

	ttl := client.TTL(ctx, key).Val()
	require.Greater(t, ttl, time.Duration(0))
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
