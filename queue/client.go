package queue

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/replicate/go/shuffleshard"
)

var (
	//go:embed len.lua
	lenCmd    string
	lenScript = redis.NewScript(lenCmd)

	//go:embed read.lua
	readCmd    string
	readScript = redis.NewScript(readCmd)

	//go:embed write.lua
	writeCmd    string
	writeScript = redis.NewScript(writeCmd)

	ErrInvalidReadArgs  = fmt.Errorf("queue: invalid read arguments")
	ErrInvalidWriteArgs = fmt.Errorf("queue: invalid write arguments")
)

func NewClient(rdb redis.Cmdable, ttl time.Duration) *Client {
	return &Client{
		rdb: rdb,
		ttl: ttl,
	}
}

// Prepare stores the write and read scripts in the Redis script cache so that
// they can be more efficiently called with EVALSHA.
func (c *Client) Prepare(ctx context.Context) error {
	if err := lenScript.Load(ctx, c.rdb).Err(); err != nil {
		return err
	}
	if err := readScript.Load(ctx, c.rdb).Err(); err != nil {
		return err
	}
	if err := writeScript.Load(ctx, c.rdb).Err(); err != nil {
		return err
	}
	return nil
}

func (c *Client) Len(ctx context.Context, name string) (int64, error) {
	return lenScript.RunRO(ctx, c.rdb, []string{name}).Int64()
}

func (c *Client) Read(ctx context.Context, a *ReadArgs) (*Message, error) {
	if a == nil {
		return nil, fmt.Errorf("%w: args cannot be nil", ErrInvalidReadArgs)
	}
	if a.Name == "" {
		return nil, fmt.Errorf("%w: name cannot be empty", ErrInvalidReadArgs)
	}
	if a.Group == "" {
		return nil, fmt.Errorf("%w: group cannot be empty", ErrInvalidReadArgs)
	}
	if a.Consumer == "" {
		return nil, fmt.Errorf("%w: consumer cannot be empty", ErrInvalidReadArgs)
	}

	return c.read(ctx, a)
}

func (c *Client) read(ctx context.Context, a *ReadArgs) (*Message, error) {
	result, err := c.readOnce(ctx, a)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	// If we get here, we got no message, so consider waiting on the notifications
	// channel for up to the block duration.
	if a.Block == 0 {
		return nil, nil
	}

	ok, err := c.wait(ctx, a)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	return c.readOnce(ctx, a)
}

func (c *Client) readOnce(ctx context.Context, a *ReadArgs) (*Message, error) {
	keys := []string{a.Name}
	args := []any{int(c.ttl.Seconds()), a.Group, a.Consumer}
	result, err := readScript.Run(ctx, c.rdb, keys, args...).Result()
	switch {
	case err == redis.Nil:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return parse(result)
}

func (c *Client) wait(ctx context.Context, a *ReadArgs) (bool, error) {
	ok, err := c.waitOnce(ctx, a)
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			stream := a.Name + ":notifications"
			if err := c.rdb.XGroupCreateMkStream(ctx, stream, a.Group, "0").Err(); err != nil {
				return false, err
			}
			// If we create the stream, we're responsible for expiring it.
			if err := c.rdb.Expire(ctx, stream, c.ttl).Err(); err != nil {
				return false, err
			}
			return c.waitOnce(ctx, a)
		}
	}
	return ok, err
}

func (c *Client) waitOnce(ctx context.Context, a *ReadArgs) (bool, error) {
	err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    a.Group,
		Consumer: a.Consumer,
		Streams:  []string{a.Name + ":notifications", ">"},
		Block:    a.Block,
		Count:    1,
		NoAck:    true, // immediately ack so no further handling is required
	}).Err()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

func parse(v any) (*Message, error) {
	result, err := parseSliceWithLength(v, 1)
	if err != nil {
		return nil, err
	}
	pair, err := parseSliceWithLength(result[0], 2)
	if err != nil {
		return nil, err
	}
	stream := pair[0].(string)
	messages, err := parseSliceWithLength(pair[1], 1)
	if err != nil {
		return nil, err
	}
	message, err := parseSliceWithLength(messages[0], 2)
	if err != nil {
		return nil, err
	}
	id := message[0].(string)
	values, err := parseMapFromSlice(message[1])
	if err != nil {
		return nil, err
	}
	return &Message{
		Stream: stream,
		ID:     id,
		Values: values,
	}, nil
}

func parseSliceWithLength(v any, length int) ([]any, error) {
	slice, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T", v)
	}
	if len(slice) != length {
		return nil, fmt.Errorf("must have length %d, got %d", length, len(slice))
	}
	return slice, nil
}

func parseMapFromSlice(v any) (map[string]any, error) {
	slice, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T", v)
	}
	if len(slice)%2 != 0 {
		return nil, fmt.Errorf("must have even length, got %d", len(slice))
	}
	m := make(map[string]any)
	for i := 0; i < len(slice); i += 2 {
		m[slice[i].(string)] = slice[i+1]
	}
	return m, nil
}
