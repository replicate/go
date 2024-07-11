package queue_test

import (
	"strconv"
	"testing"
	"time"

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

	msgid, err := client.Write(ctx, &queue.WriteArgs{
		Name:            "test",
		Streams:         16,
		StreamsPerShard: 2,
		ShardKey:        []byte("elephant"),
		Values: map[string]any{
			"hello": "world",
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, msgid)
}

func TestClientWriteIntegration(t *testing.T) {
	ctx := test.Context(t)
	mr, rdb := test.MiniRedis(t)

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
	assert.Equal(t, "2", mr.HGet("myqueue:meta", "streams"))
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
	mr.FastForward(ttl)
	assert.False(t, mr.Exists("myqueue:meta"))
	assert.False(t, mr.Exists("myqueue:notifications"))
	assert.False(t, mr.Exists("myqueue:s0"))
	assert.False(t, mr.Exists("myqueue:s1"))
}
