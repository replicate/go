//go:build integration

package kv_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/replicate/go/kv"
)

func TestMain(m *testing.M) {
	// Ensure we have a Redis URL for testing
	if os.Getenv("REDIS_URL") == "" {
		os.Setenv("REDIS_URL", "redis://localhost:6379")
	}

	// Set test environment
	os.Setenv("ENVIRONMENT", "test")

	os.Exit(m.Run())
}

func TestKV_New_Integration(t *testing.T) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	require.NotEmpty(t, redisURL, "REDIS_URL environment variable is required for integration tests")

	t.Run("basic client creation", func(t *testing.T) {
		client, err := kv.New(ctx, "test-client", redisURL)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Test basic operation
		err = client.Set(ctx, "test:key", "test-value", time.Minute).Err()
		require.NoError(t, err)

		val, err := client.Get(ctx, "test:key").Result()
		require.NoError(t, err)
		assert.Equal(t, "test-value", val)

		// Cleanup
		client.Del(ctx, "test:key")
	})

	t.Run("with custom pool size", func(t *testing.T) {
		client, err := kv.New(ctx, "test-pool", redisURL, kv.WithPoolSize(15))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Just verify that the client works - we can't easily check pool size
		// through the redis.Cmdable interface
		err = client.Ping(ctx).Err()
		require.NoError(t, err)
	})

	t.Run("with enhanced logging", func(t *testing.T) {
		client, err := kv.New(ctx, "test-enhanced", redisURL, kv.WithEnhancedLogging(true))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Test operation to ensure client works
		err = client.Ping(ctx).Err()
		require.NoError(t, err)
	})

	t.Run("with custom trace sampling", func(t *testing.T) {
		client, err := kv.New(ctx, "test-tracing", redisURL, kv.WithTraceSampling(0.5))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Test operation
		err = client.Set(ctx, "test:trace", "trace-value", time.Minute).Err()
		require.NoError(t, err)

		// Cleanup
		client.Del(ctx, "test:trace")
	})

	t.Run("multiple clients with shared tracer provider", func(t *testing.T) {
		// Create first client (this will create the global tracer provider)
		client1, err := kv.New(ctx, "test-shared-1", redisURL)
		require.NoError(t, err)
		require.NotNil(t, client1)

		// Create second client (should reuse the tracer provider)
		client2, err := kv.New(ctx, "test-shared-2", redisURL)
		require.NoError(t, err)
		require.NotNil(t, client2)

		// Test both clients work
		err = client1.Set(ctx, "test:shared1", "value1", time.Minute).Err()
		require.NoError(t, err)

		err = client2.Set(ctx, "test:shared2", "value2", time.Minute).Err()
		require.NoError(t, err)

		// Verify values
		val1, err := client1.Get(ctx, "test:shared1").Result()
		require.NoError(t, err)
		assert.Equal(t, "value1", val1)

		val2, err := client2.Get(ctx, "test:shared2").Result()
		require.NoError(t, err)
		assert.Equal(t, "value2", val2)

		// Cleanup
		client1.Del(ctx, "test:shared1")
		client2.Del(ctx, "test:shared2")
	})
}

func TestKV_ValidationErrors_Integration(t *testing.T) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	require.NotEmpty(t, redisURL)

	t.Run("empty client name", func(t *testing.T) {
		client, err := kv.New(ctx, "", redisURL)
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "client name cannot be empty")
	})

	t.Run("empty redis URL", func(t *testing.T) {
		client, err := kv.New(ctx, "test-client", "")
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "redis URL cannot be empty")
	})

	t.Run("invalid redis URL", func(t *testing.T) {
		client, err := kv.New(ctx, "test-client", "invalid-url")
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "failed to parse redis URL")
	})

	t.Run("invalid pool size", func(t *testing.T) {
		client, err := kv.New(ctx, "test-client", redisURL, kv.WithPoolSize(-1))
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "pool size must be positive")
	})

	t.Run("invalid trace sampling rate", func(t *testing.T) {
		client, err := kv.New(ctx, "test-client", redisURL, kv.WithTraceSampling(1.5))
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "trace sampling rate must be between 0 and 1")
	})

	t.Run("nil tracer provider", func(t *testing.T) {
		client, err := kv.New(ctx, "test-client", redisURL, kv.WithTracerProvider(nil))
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "tracer provider cannot be nil")
	})

	t.Run("empty sentinel addresses", func(t *testing.T) {
		client, err := kv.New(ctx, "test-client", redisURL, kv.WithSentinel("mymaster", []string{}, "password"))
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "sentinel addresses cannot be empty")
	})
}

func TestKV_EnvironmentDefaults_Integration(t *testing.T) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	require.NotEmpty(t, redisURL)

	tests := []struct {
		name        string
		environment string
		expectedMin int
		expectedMax int
	}{
		{"test environment", "test", 5, 5},
		{"development environment", "development", 10, 10},
		{"staging environment", "staging", 15, 15},
		{"production environment", "production", 20, 20},
		{"unknown environment defaults to development", "unknown", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment
			originalEnv := os.Getenv("ENVIRONMENT")
			os.Setenv("ENVIRONMENT", tt.environment)
			defer os.Setenv("ENVIRONMENT", originalEnv)

			client, err := kv.New(ctx, fmt.Sprintf("test-%s", tt.environment), redisURL)
			require.NoError(t, err)
			require.NotNil(t, client)

			// Verify the client works
			err = client.Ping(ctx).Err()
			require.NoError(t, err)
		})
	}
}

func TestKV_TracerProviderCustomization_Integration(t *testing.T) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	require.NotEmpty(t, redisURL)

	t.Run("custom tracer provider", func(t *testing.T) {
		// Create a mock tracer provider
		mockProvider := trace.NewNoopTracerProvider()

		client, err := kv.New(ctx, "test-custom-tracer", redisURL, kv.WithTracerProvider(mockProvider))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Test operation
		err = client.Set(ctx, "test:custom-tracer", "value", time.Minute).Err()
		require.NoError(t, err)

		// Cleanup
		client.Del(ctx, "test:custom-tracer")
	})
}

func TestKV_ConnectionPoolBehavior_Integration(t *testing.T) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	require.NotEmpty(t, redisURL)

	t.Run("large pool size warning", func(t *testing.T) {
		// This should generate a warning log but still work
		client, err := kv.New(ctx, "test-large-pool", redisURL, kv.WithPoolSize(1001))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Verify client works
		err = client.Ping(ctx).Err()
		require.NoError(t, err)
	})

	t.Run("reasonable pool size", func(t *testing.T) {
		client, err := kv.New(ctx, "test-reasonable-pool", redisURL, kv.WithPoolSize(50))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Test concurrent operations
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func(id int) {
				key := fmt.Sprintf("test:concurrent:%d", id)
				err := client.Set(ctx, key, fmt.Sprintf("value-%d", id), time.Minute).Err()
				assert.NoError(t, err)

				val, err := client.Get(ctx, key).Result()
				assert.NoError(t, err)
				assert.Equal(t, fmt.Sprintf("value-%d", id), val)

				client.Del(ctx, key)
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			select {
			case <-done:
				// success
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for concurrent operations")
			}
		}
	})
}

func TestKV_SentinelConfiguration_Integration(t *testing.T) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	require.NotEmpty(t, redisURL)

	t.Run("sentinel with empty primary name", func(t *testing.T) {
		// Should succeed but not configure sentinel
		client, err := kv.New(ctx, "test-no-sentinel", redisURL, kv.WithSentinel("", []string{"sentinel:26379"}, "password"))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Verify client works normally
		err = client.Ping(ctx).Err()
		require.NoError(t, err)
	})
}

func TestKV_TLSConfiguration_Integration(t *testing.T) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	require.NotEmpty(t, redisURL)

	t.Run("TLS with non-TLS URL", func(t *testing.T) {
		// Should succeed but not configure TLS since URL doesn't specify TLS
		client, err := kv.New(ctx, "test-no-tls", redisURL, kv.WithAutoTLS("/nonexistent/ca.crt"))
		require.NoError(t, err)
		require.NotNil(t, client)

		// Verify client works
		err = client.Ping(ctx).Err()
		require.NoError(t, err)
	})
}

// BenchmarkKV_ClientCreation benchmarks the performance of creating Redis clients
func BenchmarkKV_ClientCreation(b *testing.B) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		b.Skip("REDIS_URL not set, skipping benchmark")
	}

	b.Run("basic client creation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			client, err := kv.New(ctx, fmt.Sprintf("bench-%d", i), redisURL)
			if err != nil {
				b.Fatal(err)
			}
			// Close the client to avoid resource leaks
			if closer, ok := client.(interface{ Close() error }); ok {
				closer.Close()
			}
		}
	})

	b.Run("client creation with options", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			client, err := kv.New(ctx, fmt.Sprintf("bench-opts-%d", i), redisURL,
				kv.WithPoolSize(20),
				kv.WithTraceSampling(0.1),
				kv.WithEnhancedLogging(true),
			)
			if err != nil {
				b.Fatal(err)
			}
			if closer, ok := client.(interface{ Close() error }); ok {
				closer.Close()
			}
		}
	})
}

// BenchmarkKV_Operations benchmarks basic Redis operations
func BenchmarkKV_Operations(b *testing.B) {
	ctx := context.Background()
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		b.Skip("REDIS_URL not set, skipping benchmark")
	}

	client, err := kv.New(ctx, "bench-operations", redisURL)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("set operations", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:set:%d", i)
			err := client.Set(ctx, key, "benchmark-value", time.Minute).Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("get operations", func(b *testing.B) {
		// Pre-populate some keys
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("bench:get:%d", i)
			client.Set(ctx, key, "benchmark-value", time.Minute)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:get:%d", i%100)
			_, err := client.Get(ctx, key).Result()
			if err != nil && err != redis.Nil {
				b.Fatal(err)
			}
		}
	})
}
