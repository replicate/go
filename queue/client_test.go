package queue_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
