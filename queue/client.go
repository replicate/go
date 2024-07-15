package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/replicate/go/shuffleshard"
)

var (
	ErrInvalidReadArgs  = fmt.Errorf("queue: invalid read arguments")
	ErrInvalidWriteArgs = fmt.Errorf("queue: invalid write arguments")
)

type Client struct {
	rdb redis.Cmdable
	ttl time.Duration // ttl for all keys in queue
}

func NewClient(rdb redis.Cmdable, ttl time.Duration) *Client {
	return &Client{
		rdb: rdb,
		ttl: ttl,
	}
}

// Prepare stores the write and read scripts in the Redis script cache so that
// they can be more efficiently called with EVALSHA.
func (c *Client) Prepare(ctx context.Context) error {
	return prepare(ctx, c.rdb)
}

// Len calculates the aggregate length (XLEN) of the queue. It adds up the
// lengths of all the streams in the queue.
func (c *Client) Len(ctx context.Context, name string) (int64, error) {
	return lenScript.RunRO(ctx, c.rdb, []string{name}).Int64()
}

// Read a single message from the queue. If the Block field of args is
// non-zero, the call may block for up to that duration waiting for a new
// message.
//
// If no message is available both return values will be nil.
func (c *Client) Read(ctx context.Context, args *ReadArgs) (*Message, error) {
	if args == nil {
		return nil, fmt.Errorf("%w: args cannot be nil", ErrInvalidReadArgs)
	}
	if args.Name == "" {
		return nil, fmt.Errorf("%w: name cannot be empty", ErrInvalidReadArgs)
	}
	if args.Group == "" {
		return nil, fmt.Errorf("%w: group cannot be empty", ErrInvalidReadArgs)
	}
	if args.Consumer == "" {
		return nil, fmt.Errorf("%w: consumer cannot be empty", ErrInvalidReadArgs)
	}

	return c.read(ctx, args)
}

func (c *Client) read(ctx context.Context, args *ReadArgs) (*Message, error) {
	result, err := c.readOnce(ctx, args)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}

	// If we're here, the queue was Empty. We only have more work to do if
	// args.Block is non-zero.
	if args.Block == 0 {
		return nil, nil
	}

	// Wait for a message to be signaled on the notifications stream.
	ok, err := c.wait(ctx, args)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	return c.readOnce(ctx, args)
}

func (c *Client) readOnce(ctx context.Context, args *ReadArgs) (*Message, error) {
	cmdKeys := []string{args.Name}
	cmdArgs := []any{int(c.ttl.Seconds()), args.Group, args.Consumer}
	result, err := readScript.Run(ctx, c.rdb, cmdKeys, cmdArgs...).Result()
	switch {
	case err == redis.Nil:
		return nil, nil
	case err != nil:
		return nil, err
	}

	return parse(result)
}

func (c *Client) wait(ctx context.Context, args *ReadArgs) (bool, error) {
	ok, err := c.waitOnce(ctx, args)
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			stream := args.Name + ":notifications"
			if err := c.rdb.XGroupCreateMkStream(ctx, stream, args.Group, "0").Err(); err != nil {
				return false, err
			}
			// If we create the stream, we're responsible for expiring it.
			if err := c.rdb.Expire(ctx, stream, c.ttl).Err(); err != nil {
				return false, err
			}
			return c.waitOnce(ctx, args)
		}
	}
	return ok, err
}

func (c *Client) waitOnce(ctx context.Context, args *ReadArgs) (bool, error) {
	err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    args.Group,
		Consumer: args.Consumer,
		Streams:  []string{args.Name + ":notifications", ">"},
		Block:    args.Block,
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

// Write a message to the queue. The message will be written to the shortest
// queue in the tenant's shard, which is determined by the ShardKey in args.
func (c *Client) Write(ctx context.Context, args *WriteArgs) (string, error) {
	if args == nil {
		return "", fmt.Errorf("%w: args cannot be nil", ErrInvalidWriteArgs)
	}
	if args.Name == "" {
		return "", fmt.Errorf("%w: name cannot be empty", ErrInvalidWriteArgs)
	}
	if args.Streams == 0 {
		args.Streams = 1
	}
	if args.StreamsPerShard == 0 {
		args.StreamsPerShard = 1
	}
	if args.Streams < 0 {
		return "", fmt.Errorf("%w: streams must be > 0", ErrInvalidWriteArgs)
	}
	if args.StreamsPerShard < 0 {
		return "", fmt.Errorf("%w: streams per shard must be > 0", ErrInvalidWriteArgs)
	}
	if args.StreamsPerShard > args.Streams {
		return "", fmt.Errorf("%w: streams per shard must be <= streams", ErrInvalidWriteArgs)
	}
	if len(args.ShardKey) == 0 {
		return "", fmt.Errorf("%w: shard key cannot be empty", ErrInvalidWriteArgs)
	}
	if len(args.Values) == 0 {
		return "", fmt.Errorf("%w: values cannot be empty", ErrInvalidWriteArgs)
	}

	return c.write(ctx, args)
}

func (c *Client) write(ctx context.Context, args *WriteArgs) (string, error) {
	shard := shuffleshard.Get(args.Streams, args.StreamsPerShard, args.ShardKey)

	cmdKeys := []string{args.Name}
	// Capacity: 3 (for seconds, streams, n) + len(shard) + 2*len(values)
	cmdArgs := make([]any, 0, 3+len(shard)+2*len(args.Values))

	cmdArgs = append(cmdArgs, int(c.ttl.Seconds()))
	cmdArgs = append(cmdArgs, args.Streams)
	cmdArgs = append(cmdArgs, len(shard))
	for _, s := range shard {
		cmdArgs = append(cmdArgs, s)
	}
	for k, v := range args.Values {
		cmdArgs = append(cmdArgs, k, v)
	}

	return writeScript.Run(ctx, c.rdb, cmdKeys, cmdArgs...).Text()
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
