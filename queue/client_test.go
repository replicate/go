package queue_test

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	exprand "golang.org/x/exp/rand"

	"github.com/replicate/go/queue"
	"github.com/replicate/go/test"
)

func TestClientIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(t, client.Prepare(ctx))

	id := 0

	for range 10 {
		_, err := client.Write(ctx, &queue.WriteArgs{
			Name:            "test",
			Streams:         16,
			StreamsPerShard: 2,
			ShardKey:        []byte("elephant"),
			Values: map[string]any{
				"type": "mammal",
				"id":   id,
			},
		})
		require.NoError(t, err)
		id++
	}
	for range 5 {
		_, err := client.Write(ctx, &queue.WriteArgs{
			Name:            "test",
			Streams:         16,
			StreamsPerShard: 2,
			ShardKey:        []byte("tuna"),
			Values: map[string]any{
				"type": "fish",
				"id":   id,
			},
		})
		require.NoError(t, err)
		id++
	}

	ids := make(map[string]struct{})

	for range 15 {
		msg, err := client.Read(ctx, &queue.ReadArgs{
			Name:     "test",
			Group:    "mygroup",
			Consumer: "mygroup:123",
		})
		require.NoError(t, err)
		require.NotNil(t, msg)
		assert.Contains(t, msg.Values, "type")
		assert.Contains(t, msg.Values, "id")
		ids[msg.Values["id"].(string)] = struct{}{}
	}

	// We should have read all the messages we enqueued
	assert.Len(t, ids, 15)

	// And there should be no more messages to read
	msg, err := client.Read(ctx, &queue.ReadArgs{
		Name:     "test",
		Group:    "mygroup",
		Consumer: "mygroup:123",
	})
	require.NoError(t, err)
	require.Nil(t, msg)
}

// Check that the Block option works as expected
func TestClientBlockIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(t, client.Prepare(ctx))

	result := make(chan *queue.Message, 1)

	go func() {
		msg, err := client.Read(ctx, &queue.ReadArgs{
			Name:     "test",
			Group:    "mygroup",
			Consumer: "mygroup:123",
			Block:    time.Second,
		})
		require.NoError(t, err)
		require.NotNil(t, msg)
		result <- msg
	}()

	time.Sleep(100 * time.Millisecond)
	_, err := client.Write(ctx, &queue.WriteArgs{
		Name:            "test",
		Streams:         16,
		StreamsPerShard: 2,
		ShardKey:        []byte("tuna"),
		Values:          map[string]any{"type": "fish"},
	})
	require.NoError(t, err)

	select {
	case msg := <-result:
		assert.Equal(t, "fish", msg.Values["type"])
	case <-time.After(10 * time.Millisecond):
		t.Fatal("expected read to have succeeded")
	}
}

func TestClientReadIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(t, client.Prepare(ctx))

	// Prepare a queue with 4 streams
	require.NoError(t, rdb.HSet(ctx, "myqueue:meta", "streams", 4).Err())

	for i := range 4 {
		for j := range 10 {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: fmt.Sprintf("myqueue:s%d", i),
				Values: map[string]any{
					"idx": fmt.Sprintf("%d-%d", i, j),
				},
			}).Err())
		}
	}

	msgs := make(map[string]struct{})
	for {
		msg, err := client.Read(ctx, &queue.ReadArgs{
			Name:     "myqueue",
			Group:    "mygroup",
			Consumer: "mygroup:123",
		})
		require.NoError(t, err)
		if msg == nil {
			break
		}
		msgs[msg.Values["idx"].(string)] = struct{}{}
	}

	assert.Len(t, msgs, 40)
}

func TestClientReadLegacyStreamIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(t, client.Prepare(ctx))

	// Prepare a queue with 4 streams
	require.NoError(t, rdb.HSet(ctx, "myqueue:meta", "streams", 4).Err())

	// But also populate the default stream
	for i := range 10 {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "myqueue",
			Values: map[string]any{
				"idx": fmt.Sprintf("default-%d", i),
			},
		}).Err())
	}

	for i := range 4 {
		for j := range 10 {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: fmt.Sprintf("myqueue:s%d", i),
				Values: map[string]any{
					"idx": fmt.Sprintf("%d-%d", i, j),
				},
			}).Err())
		}
	}

	msgs := make(map[string]struct{})
	for {
		msg, err := client.Read(ctx, &queue.ReadArgs{
			Name:     "myqueue",
			Group:    "mygroup",
			Consumer: "mygroup:123",
		})
		require.NoError(t, err)
		if msg == nil {
			break
		}
		msgs[msg.Values["idx"].(string)] = struct{}{}
	}

	assert.Len(t, msgs, 50)
}

func TestClientWriteIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(t, client.Prepare(ctx))

	for i := range 10 {
		_, err := client.Write(ctx, &queue.WriteArgs{
			Name:            "myqueue",
			Streams:         2,
			StreamsPerShard: 1,
			ShardKey:        []byte("panda"),
			Values: map[string]any{
				"name": "panda",
				"idx":  i,
			},
		})
		require.NoError(t, err)
	}

	for i := range 5 {
		_, err := client.Write(ctx, &queue.WriteArgs{
			Name:            "myqueue",
			Streams:         2,
			StreamsPerShard: 1,
			ShardKey:        []byte("giraffe"),
			Values: map[string]any{
				"name": "giraffe",
				"idx":  i,
			},
		})
		require.NoError(t, err)
	}

	// meta key contains the number of streams
	val, err := rdb.HGet(ctx, "myqueue:meta", "streams").Result()
	require.NoError(t, err)
	assert.Equal(t, "2", val)
	// notification stream contains at least one message
	{
		ln, err := rdb.XLen(ctx, "myqueue:notifications").Result()
		require.NoError(t, err)
		assert.Greater(t, ln, int64(0))
	}

	// "giraffe" is assigned to shard 0
	{
		l0, err := rdb.XLen(ctx, "myqueue:s0").Result()
		require.NoError(t, err)
		assert.EqualValues(t, 5, l0)

		values, err := rdb.XRange(ctx, "myqueue:s0", "-", "+").Result()
		require.NoError(t, err)
		assert.Len(t, values, 5)
		for i, v := range values {
			assert.Equal(t, map[string]interface{}{
				"name": "giraffe",
				"idx":  strconv.Itoa(i),
			}, v.Values)
		}
	}

	// "panda" is assigned to shard 1
	{
		l1, err := rdb.XLen(ctx, "myqueue:s1").Result()
		require.NoError(t, err)
		assert.EqualValues(t, 10, l1)

		values, err := rdb.XRange(ctx, "myqueue:s1", "-", "+").Result()
		require.NoError(t, err)
		assert.Len(t, values, 10)
		for i, v := range values {
			assert.Equal(t, map[string]interface{}{
				"name": "panda",
				"idx":  strconv.Itoa(i),
			}, v.Values)
		}
	}

	// expiry should be set on all keys
	keys := []string{
		"myqueue:meta",
		"myqueue:notifications",
		"myqueue:s0",
		"myqueue:s1",
	}

	for _, key := range keys {
		ttl, err := rdb.TTL(ctx, key).Result()
		require.NoError(t, err)
		assert.Greater(t, ttl, 23*time.Hour)
	}
}

// TestPickupLatencyIntegration runs a test with a mostly-empty queue -- by
// running artificially slow producers and full-speed consumers -- to ensure
// that the blocking read operation has low latency.
//
// This is primarily a test of the notification mechanism, which should wake up
// waiting consumers as soon as a message is available.
func TestPickupLatencyIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(t, client.Prepare(ctx))

	n := runtime.GOMAXPROCS(0)
	mark := time.Now()
	runDuration := 10 * time.Second
	producerRate := 10.0

	var wg sync.WaitGroup

	// Start n workers emitting messages
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			key := make([]byte, 16)
			_, _ = crand.Read(key)
			for time.Since(mark) < runDuration {
				_, err := client.Write(ctx, &queue.WriteArgs{
					Name:            "testqueue",
					Streams:         16,
					StreamsPerShard: 4,
					ShardKey:        key,
					Values:          map[string]any{"t": time.Now().UnixNano()},
				})
				require.NoError(t, err)
				wait := exprand.ExpFloat64() / producerRate
				time.Sleep(time.Duration(wait * float64(time.Second)))
			}
			wg.Done()
		}()
	}

	// Start n workers consuming messages
	var totalMessages atomic.Int64
	var totalLatency atomic.Int64

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			for time.Since(mark) < runDuration {
				msg, err := client.Read(ctx, &queue.ReadArgs{
					Name:     "testqueue",
					Group:    "reader",
					Consumer: fmt.Sprintf("reader:%d", i),
					Block:    100 * time.Millisecond,
				})
				// Clients racing each other to create the consumer group isn't a big
				// deal in production so we ignore it.
				if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
					t.Error(err)
				}

				if msg == nil {
					continue
				}

				nanos, _ := strconv.ParseInt(msg.Values["t"].(string), 10, 64)
				enqueuedAt := time.Unix(0, nanos)
				totalMessages.Add(1)
				totalLatency.Add(time.Since(enqueuedAt).Nanoseconds())
			}
			wg.Done()
		}()
	}

	wg.Wait()

	messages := totalMessages.Load()
	meanLatency := time.Duration(float64(totalLatency.Load()) / float64(totalMessages.Load()))

	t.Logf("messages delivered: %d\n", messages)
	t.Logf("mean pickup latency: %s\n", meanLatency)

	expectedMessages := float64(n) * runDuration.Seconds() * producerRate
	assert.InEpsilon(t, expectedMessages, messages, 0.1) // 10% relative error
	assert.Less(t, meanLatency, 5*time.Millisecond)
}

func benchmarkRead(streams int, b *testing.B) {
	ctx := test.Context(b)
	rdb := test.Redis(ctx, b)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(b, client.Prepare(ctx))

	// Prepare queues
	require.NoError(b, rdb.HSet(ctx, "testbench:meta", "streams", streams).Err())

	for i := range b.N {
		require.NoError(b, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: fmt.Sprintf("testbench:s%d", i%streams),
			Values: map[string]any{"id": time.Now().UnixNano()},
		}).Err())
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := rand.Intn(8192)
		for pb.Next() {
			msg, err := client.Read(ctx, &queue.ReadArgs{
				Name:     "testbench",
				Group:    "benchmark",
				Consumer: fmt.Sprintf("benchmark:%d", id),
			})
			require.NoError(b, err)
			require.NotNil(b, msg)
		}
	})
}

func BenchmarkRead(b *testing.B) {
	b.Run("1", func(b *testing.B) { benchmarkRead(1, b) })
	b.Run("16", func(b *testing.B) { benchmarkRead(16, b) })
	b.Run("64", func(b *testing.B) { benchmarkRead(64, b) })
}

func benchmarkWrite(streams, streamsPerShard int, b *testing.B) {
	ctx := test.Context(b)
	rdb := test.Redis(ctx, b)

	ttl := 24 * time.Hour
	client := queue.NewClient(rdb, ttl)
	require.NoError(b, client.Prepare(ctx))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 16)
		_, _ = crand.Read(key)
		for pb.Next() {
			_, err := client.Write(ctx, &queue.WriteArgs{
				Name:            "testbench",
				Streams:         streams,
				StreamsPerShard: streamsPerShard,
				ShardKey:        key,
				Values:          map[string]any{"id": time.Now().UnixNano()},
			})
			require.NoError(b, err)
		}
	})
}

func BenchmarkWrite(b *testing.B) {
	b.Run("1-1", func(b *testing.B) { benchmarkWrite(1, 1, b) })
	b.Run("16-4", func(b *testing.B) { benchmarkWrite(16, 4, b) })
	b.Run("64-6", func(b *testing.B) { benchmarkWrite(64, 6, b) })
}
