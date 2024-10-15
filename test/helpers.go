package test

import (
	"context"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func Context(t testing.TB) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return ctx
}

func Redis(ctx context.Context, t testing.TB) *redis.Client {
	t.Helper()

	opts, err := redis.ParseURL(RedisURL(t))
	if err != nil {
		t.Fatalf("failed to parse redis url: %v", err)
	}

	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	// Reset the database
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal("failed to flush db")
	}

	return rdb
}

func RedisURL(t testing.TB) string {
	t.Helper()

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not set")
	}

	return redisURL
}

func MiniRedis(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return mr, rdb
}
