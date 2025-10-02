package queue_test

import (
	"context"
	crand "crypto/rand"
	"crypto/sha1"
	"errors"
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
	"github.com/replicate/go/uuid"
)

func TestClientIntegration(t *testing.T) {
	t.Run("vanilla", func(t *testing.T) {
		ctx := test.Context(t)
		rdb := test.Redis(ctx, t)

		ttl := 24 * time.Hour
		client := queue.NewClient(rdb, ttl)
		require.NoError(t, client.Prepare(ctx))

		runClientIntegrationTest(ctx, t, client)
	})

	t.Run("with-tracking", func(t *testing.T) {
		ctx := test.Context(t)
		rdb := test.Redis(ctx, t)

		ttl := 24 * time.Hour
		client := queue.NewTrackingClient(rdb, ttl, "id")
		require.NoError(t, client.Prepare(ctx))

		runClientIntegrationTest(ctx, t, client)
	})
}

func runClientIntegrationTest(ctx context.Context, t *testing.T, client *queue.Client) {
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

	length, err := client.Len(ctx, "test")
	require.NoError(t, err)
	assert.EqualValues(t, 15, length)

	ids := make(map[string]struct{})

	for i := range 15 {
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

		stats, err := client.Stats(ctx, "test", "mygroup")
		require.NoError(t, err)
		assert.EqualValues(t, i+1, stats.PendingCount)
		assert.EqualValues(t, 15, stats.Len)
	}

	// We should have read all the messages we enqueued
	assert.Len(t, ids, 15)

	// And there should be no more messages to read
	_, err = client.Read(ctx, &queue.ReadArgs{
		Name:     "test",
		Group:    "mygroup",
		Consumer: "mygroup:123",
	})

	require.ErrorIs(t, err, queue.Empty)

	// Len remains 15 (because we haven't XDELed the messages or XTRIMed the stream)
	length, err = client.Len(ctx, "test")
	require.NoError(t, err)
	assert.EqualValues(t, 15, length)
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

func messageOrderDefault(queues, messagesPerQueue int) []string {
	// We expect to read one message from each stream in turn.
	expected := make([]string, 0, queues*messagesPerQueue)
	for message := range messagesPerQueue {
		for queue := range queues {
			expected = append(expected, fmt.Sprintf("%d-%d", queue, message))
		}
	}
	return expected
}

func messageOrderPreferredStream(queues, messagesPerQueue int) []string {
	// We expect to read all messages from queue 0 before moving on to queue 1,
	// etc.
	expected := make([]string, 0, queues*messagesPerQueue)
	for queue := range queues {
		for message := range messagesPerQueue {
			expected = append(expected, fmt.Sprintf("%d-%d", queue, message))
		}
	}
	return expected
}

func TestClientReadIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	testcases := []struct {
		Name            string
		Block           time.Duration
		TrackLastStream bool
		ExpectFn        func(queues, messagesPerQueue int) []string
	}{
		{
			Name:     "Default (non-blocking)",
			ExpectFn: messageOrderDefault,
		},
		{
			Name:     "Default (blocking)",
			Block:    10 * time.Millisecond,
			ExpectFn: messageOrderDefault,
		},
		{
			Name:            "PreferStream (non-blocking)",
			TrackLastStream: true,
			ExpectFn:        messageOrderPreferredStream,
		},
		{
			Name:            "PreferStream (blocking)",
			Block:           10 * time.Millisecond,
			TrackLastStream: true,
			ExpectFn:        messageOrderPreferredStream,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			queues := 4
			messagesPerQueue := 10
			ttl := 24 * time.Hour
			client := queue.NewClient(rdb, ttl)
			require.NoError(t, client.Prepare(ctx))

			// Prepare a queue
			require.NoError(t, rdb.HSet(ctx, "myqueue:meta", "streams", queues).Err())

			for i := range queues {
				for j := range messagesPerQueue {
					require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
						Stream: fmt.Sprintf("myqueue:s%d", i),
						Values: map[string]any{
							"idx": fmt.Sprintf("%d-%d", i, j),
						},
					}).Err())
				}
			}

			var lastStream string
			msgs := make([]string, 0, queues*messagesPerQueue)
			for {
				readArgs := &queue.ReadArgs{
					Name:     "myqueue",
					Group:    "mygroup",
					Consumer: "mygroup:123",
					Block:    tc.Block,
				}
				if tc.TrackLastStream {
					readArgs.PreferStream = lastStream
				}
				msg, err := client.Read(ctx, readArgs)
				if errors.Is(err, queue.Empty) {
					break
				}
				require.NoError(t, err)
				lastStream = msg.Stream
				msgs = append(msgs, msg.Values["idx"].(string))
			}

			expected := tc.ExpectFn(queues, messagesPerQueue)

			assert.Len(t, msgs, queues*messagesPerQueue)
			assert.EqualValues(t, expected, msgs)
		})
	}
}

func TestClientWriteIntegration(t *testing.T) {
	t.Run("vanilla", func(t *testing.T) {
		ctx := test.Context(t)
		rdb := test.Redis(ctx, t)

		ttl := 24 * time.Hour
		client := queue.NewClient(rdb, ttl)
		require.NoError(t, client.Prepare(ctx))

		runClientWriteIntegrationTest(ctx, t, rdb, client, false)
	})

	t.Run("with-tracking", func(t *testing.T) {
		ctx := test.Context(t)
		rdb := test.Redis(ctx, t)

		ttl := 24 * time.Hour
		client := queue.NewTrackingClient(rdb, ttl, "tracketytrack")
		require.NoError(t, client.Prepare(ctx))

		runClientWriteIntegrationTest(ctx, t, rdb, client, true)
	})
}

func runClientWriteIntegrationTest(ctx context.Context, t *testing.T, rdb *redis.Client, client *queue.Client, withTracking bool) {
	trackIDs := []string{}

	for i := range 10 {
		trackID, err := uuid.NewV7()
		require.NoError(t, err)

		trackIDs = append(trackIDs, trackID.String())

		_, err = client.Write(ctx, &queue.WriteArgs{
			Name:            "myqueue",
			Streams:         2,
			StreamsPerShard: 1,
			ShardKey:        []byte("panda"),
			Values: map[string]any{
				"idx":           i,
				"name":          "panda",
				"tracketytrack": trackID.String(),
			},
		})
		require.NoError(t, err)
	}

	for i := range 5 {
		trackID, err := uuid.NewV7()
		require.NoError(t, err)

		trackIDs = append(trackIDs, trackID.String())

		_, err = client.Write(ctx, &queue.WriteArgs{
			Name:            "myqueue",
			Streams:         2,
			StreamsPerShard: 1,
			ShardKey:        []byte("giraffe"),
			Values: map[string]any{
				"idx":           i,
				"name":          "giraffe",
				"tracketytrack": trackID.String(),
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
			assert.Contains(t, v.Values, "name")
			assert.Contains(t, v.Values, "idx")
			assert.Equal(t, v.Values["name"], "giraffe")
			assert.Equal(t, v.Values["idx"], strconv.Itoa(i))
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
			assert.Contains(t, v.Values, "name")
			assert.Contains(t, v.Values, "idx")
			assert.Equal(t, v.Values["name"], "panda")
			assert.Equal(t, v.Values["idx"], strconv.Itoa(i))
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

	if !withTracking {
		return
	}

	for _, trackID := range trackIDs {
		require.NoError(t, client.Del(ctx, trackID))
	}
}

func TestClientDelIntegration(t *testing.T) {
	ctx := test.Context(t)
	rdb := test.Redis(ctx, t)

	ttl := 24 * time.Hour
	client := queue.NewTrackingClient(rdb, ttl, "tracketytrack")
	require.NoError(t, client.Prepare(ctx))

	trackIDs := []string{}

	for i := range 3 {
		trackID, err := uuid.NewV7()
		require.NoError(t, err)

		trackIDs = append(trackIDs, trackID.String())

		_, err = client.Write(ctx, &queue.WriteArgs{
			Name:            "myqueue",
			Streams:         2,
			StreamsPerShard: 1,
			ShardKey:        []byte("capybara"),
			Values: map[string]any{
				"idx":           i,
				"name":          "capybara",
				"tracketytrack": trackID.String(),
			},
		})
		require.NoError(t, err)
	}

	require.NoError(t, client.Del(ctx, trackIDs[0]))
	require.Error(t, client.Del(ctx, trackIDs[0]))
	require.Error(t, client.Del(ctx, trackIDs[0]+"oops"))
	require.Error(t, client.Del(ctx, "bogustown"))

	metaCancelationKey := "_meta:cancelation:" + fmt.Sprintf("%x", sha1.Sum([]byte(trackIDs[1])))

	metaCancel, err := rdb.Get(ctx, metaCancelationKey).Result()
	require.NoError(t, err)

	rdb.SetEx(ctx, metaCancelationKey, "{{[,"+metaCancel, 5*time.Second)

	require.Error(t, client.Del(ctx, trackIDs[1]))

	require.NoError(t, client.Del(ctx, trackIDs[2]))
	require.ErrorIs(t, client.Del(ctx, trackIDs[2]), queue.ErrNoMatchingMessageInStream)
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
				switch {
				case errors.Is(err, queue.Empty):
					continue
				case err != nil:
					// Clients racing each other to create the consumer group isn't a big
					// deal in production so we ignore it.
					if err.Error() != "BUSYGROUP Consumer Group name already exists" {
						t.Error(err)
					}
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
