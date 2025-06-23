package kv_test

import (
	"context"
	"crypto/tls"
	_ "embed"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/go/kv"
	"github.com/replicate/go/test"
)

var (
	//go:embed testdata/server.crt
	serverCrt []byte

	//go:embed testdata/server.key
	serverKey []byte

	//go:embed testdata/ca.crt
	caCrt []byte
)

func testServerTLS(t *testing.T) *tls.Config {
	cert, err := tls.X509KeyPair(serverCrt, serverKey)
	if err != nil {
		t.Fatal(err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
	}
}

func testCAFile(t *testing.T) string {
	d := t.TempDir()
	dest := filepath.Join(d, "ca.crt")

	require.NoError(
		t,
		os.WriteFile(dest, caCrt, 0o644),
	)

	return dest
}

// TestNewInvalidURL verifies that calling New with an invalid URL returns an error.
func TestNewInvalidURL(t *testing.T) {
	_, err := kv.New(t.Context(), "invalid_test", "invalid://")
	assert.Error(t, err)
}

// TestNewWithPoolSize verifies that client options (like WithPoolSize) are applied and
// the client can ping.
func TestNewWithPoolSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	redisURL := test.MiniRedisTLSURL(t, testServerTLS(t))

	client, err := kv.New(
		ctx,
		"poolsize_test",
		redisURL,
		kv.WithPoolSize(20),
		kv.WithSentinel("", nil, ""),
		kv.WithAutoTLS(testCAFile(t)),
	)
	require.NoError(t, err)

	// Test the connectivity by sending PING.
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)
}

func TestNewWithPoolSizeFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	s, _ := test.MiniSentinel(t, "mymaster")
	redisURL := test.MiniSentinelURL(t, "mymaster")
	client, err := kv.New(
		ctx,
		"poolsize_with_failover_test",
		redisURL,
		kv.WithPoolSize(20),
		kv.WithSentinel("mymaster", []string{s.Addr()}, ""),
		kv.WithAutoTLS(""),
	)
	require.NoError(t, err)

	// Test the connectivity by sending PING.
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)
}

// TestNewBasic tests basic Redis connectivity without options
func TestNewBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	redisURL := test.MiniRedisURL(t)

	client, err := kv.New(ctx, "basic_test", redisURL)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Test connectivity
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)

	// Test basic functionality
	err = client.Set(ctx, "test-key", "test-value", time.Minute).Err()
	require.NoError(t, err)

	val, err := client.Get(ctx, "test-key").Result()
	require.NoError(t, err)
	assert.Equal(t, "test-value", val)
}

// TestNewWithTLS tests TLS configuration
func TestNewWithTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	redisURL := test.MiniRedisTLSURL(t, testServerTLS(t))

	client, err := kv.New(
		ctx,
		"tls_test",
		redisURL,
		kv.WithAutoTLS(testCAFile(t)),
	)
	require.NoError(t, err)

	// Test the connectivity by sending PING.
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)
}

// TestNewWithSentinel tests Sentinel configuration
func TestNewWithSentinel(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	s, _ := test.MiniSentinel(t, "mymaster")
	redisURL := test.MiniSentinelURL(t, "mymaster")
	client, err := kv.New(
		ctx,
		"sentinel_test",
		redisURL,
		kv.WithSentinel("mymaster", []string{s.Addr()}, ""),
	)
	require.NoError(t, err)

	// Test the connectivity by sending PING.
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)

	// Test functionality - sentinel setup should work properly
	err = client.Set(ctx, "sentinel-test", "value", time.Minute).Err()
	require.NoError(t, err)

	val, err := client.Get(ctx, "sentinel-test").Result()
	require.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestNewWithSentinelIgnored tests that empty primary name ignores sentinel options
func TestNewWithSentinelIgnored(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	redisURL := test.MiniRedisURL(t)

	// Test with empty primary name - sentinel options should be ignored
	client, err := kv.New(ctx, "sentinel_ignored_test", redisURL,
		kv.WithSentinel("", []string{"127.0.0.1:26379", "127.0.0.1:26380"}, "sentinelpass"))

	require.NoError(t, err)
	require.NotNil(t, client)

	// Test functionality - should work normally with ignored sentinel options
	err = client.Set(ctx, "ignored-test", "value", time.Minute).Err()
	require.NoError(t, err)

	val, err := client.Get(ctx, "ignored-test").Result()
	require.NoError(t, err)
	assert.Equal(t, "value", val)

	// Test basic functionality
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)
}

// TestNewWithNoop tests that Noop option doesn't affect functionality
func TestNewWithNoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	redisURL := test.MiniRedisURL(t)

	client, err := kv.New(ctx, "noop_test", redisURL, kv.Noop())
	require.NoError(t, err)
	require.NotNil(t, client)

	// Test basic functionality
	pong, err := client.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)
}

// TestNewWithEmptyAddr tests connection failure
func TestNewWithEmptyAddr(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	// Test a scenario where we can't connect (connection refused)
	client, err := kv.New(ctx, "test-client", "redis://localhost:9999")
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "failed to ping")
}
