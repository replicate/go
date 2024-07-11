package queue

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/replicate/go/shuffleshard"
)

var (
	//go:embed write.lua
	writeCmd    string
	writeScript = redis.NewScript(writeCmd)

	ErrInvalidWriteArgs = fmt.Errorf("queue: invalid write arguments")
)

func NewClient(rdb redis.Cmdable, ttl time.Duration) *Client {
	return &Client{
		rdb: rdb,
		ttl: ttl,
	}
}

// Prepare stores the write and read scripts in the Redis script cache so that
// it can be more efficiently called with EVALSHA.
func (c *Client) Prepare(ctx context.Context) error {
	return writeScript.Load(ctx, c.rdb).Err()
}

func (c *Client) Write(ctx context.Context, a *WriteArgs) (string, error) {
	if a == nil {
		return "", fmt.Errorf("%w: args cannot be nil", ErrInvalidWriteArgs)
	}
	if a.Name == "" {
		return "", fmt.Errorf("%w: name cannot be empty", ErrInvalidWriteArgs)
	}
	if a.Streams == 0 {
		a.Streams = 1
	}
	if a.StreamsPerShard == 0 {
		a.StreamsPerShard = 1
	}
	if a.Streams < 0 {
		return "", fmt.Errorf("%w: streams must be > 0", ErrInvalidWriteArgs)
	}
	if a.StreamsPerShard < 0 {
		return "", fmt.Errorf("%w: streams per shard must be > 0", ErrInvalidWriteArgs)
	}
	if a.StreamsPerShard > a.Streams {
		return "", fmt.Errorf("%w: streams per shard must be <= streams", ErrInvalidWriteArgs)
	}
	if len(a.ShardKey) == 0 {
		return "", fmt.Errorf("%w: shard key cannot be empty", ErrInvalidWriteArgs)
	}
	if len(a.Values) == 0 {
		return "", fmt.Errorf("%w: values cannot be empty", ErrInvalidWriteArgs)
	}

	return c.write(ctx, a)
}

func (c *Client) write(ctx context.Context, a *WriteArgs) (string, error) {
	shard := shuffleshard.Get(a.Streams, a.StreamsPerShard, a.ShardKey)

	// Args is of length 3 (for seconds, streams, n) + len(shard) + 2*len(values)
	keys := []string{a.Name}
	args := make([]any, 0, 3+len(shard)+2*len(a.Values))

	args = append(args, int(c.ttl.Seconds()))
	args = append(args, a.Streams)
	args = append(args, len(shard))
	for _, s := range shard {
		args = append(args, s)
	}
	for k, v := range a.Values {
		args = append(args, k, v)
	}

	return writeScript.Run(ctx, c.rdb, keys, args...).Text()
}
