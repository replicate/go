package test

import (
	"context"
	"crypto/tls"
	"os"
	"testing"

	"github.com/Bose/minisentinel"
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

// MiniRedisTLS creates a miniredis instance with TLS enabled and returns the server and client
func MiniRedisTLS(t testing.TB, tlsConfig *tls.Config) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	mr, err := miniredis.RunTLS(tlsConfig)
	if err != nil {
		t.Fatalf("failed to start miniredis with TLS: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{
		Addr:      mr.Addr(),
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, // For testing
	})

	return mr, rdb
}

// MiniSentinel creates a minisentinel setup with primary and replica, returns sentinel and primary client
func MiniSentinel(t testing.TB, masterName string) (*minisentinel.Sentinel, *redis.Client) {
	t.Helper()

	pr := miniredis.RunT(t)
	r0 := miniredis.RunT(t)

	s := minisentinel.NewSentinel(
		pr,
		minisentinel.WithReplica(r0),
		minisentinel.WithMasterName(masterName),
	)
	
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start sentinel: %v", err)
	}
	t.Cleanup(s.Close)

	// Create a client that connects through sentinel
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: []string{s.Addr()},
	})
	t.Cleanup(func() { _ = rdb.Close() })

	return s, rdb
}

// MiniRedisURL creates a miniredis instance and returns its redis:// URL
func MiniRedisURL(t testing.TB) string {
	t.Helper()
	mr, _ := MiniRedis(t)
	return "redis://" + mr.Addr()
}

// MiniRedisTLSURL creates a miniredis instance with TLS and returns its rediss:// URL
func MiniRedisTLSURL(t testing.TB, tlsConfig *tls.Config) string {
	t.Helper()
	mr, _ := MiniRedisTLS(t, tlsConfig)
	return "rediss://" + mr.Addr()
}

// MiniSentinelURL creates a minisentinel instance and returns its redis:// URL
func MiniSentinelURL(t testing.TB, masterName string) string {
	t.Helper()
	s, _ := MiniSentinel(t, masterName)
	return "redis://" + s.Addr()
}
