package queue

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/replicate/go/shuffleshard"
)

var (
	ErrInvalidReadArgs  = fmt.Errorf("queue: invalid read arguments")
	ErrInvalidWriteArgs = fmt.Errorf("queue: invalid write arguments")

	streamSuffixPattern = regexp.MustCompile(`\A:s(\d+)\z`)
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

// PendingCount counts the aggregate pending entries (that is, number of
// messages that have been read but not acknowledged) of the consumer group for
// the given queue, as reported by XPENDING.
func (c *Client) PendingCount(ctx context.Context, queue string, group string) (int64, error) {
	return pendingCountScript.RunRO(ctx, c.rdb, []string{queue}, group).Int64()
}

// Read a single message from the queue. If the Block field of args is
// non-zero, the call may block for up to that duration waiting for a new
// message.
//
// If no message is available err will be [Empty].
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

	if args.PreferStream != "" {
		return c.readWithPreferredStream(ctx, args)
	}
	return c.read(ctx, args)
}

func (c *Client) readWithPreferredStream(ctx context.Context, args *ReadArgs) (*Message, error) {
	// First we validate PreferStream. If it makes sense, we'll do an XREADGROUP
	// against that stream. If it doesn't, we'll start things off with a normal
	// round-robin read.
	sid := strings.TrimPrefix(args.PreferStream, args.Name)
	if ok := streamSuffixPattern.MatchString(sid); !ok {
		return c.read(ctx, args)
	}

	// go-redis defines the behavior for the zero value of Block as blocking
	// indefinitely, which is the opposite of the default behavior of redis
	// itself. Map 0 to -1 so we get non-blocking behavior if Block is not set.
	//
	// See: https://github.com/redis/go-redis/issues/1941
	block := args.Block
	if block == 0 {
		block = -1
	}

	result, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    args.Group,
		Consumer: args.Consumer,
		Streams:  []string{args.PreferStream, ">"},
		Block:    block,
		Count:    1,
	}).Result()
	switch {
	case err == redis.Nil:
		// We try once more with a round-robin read if we got nothing from our start
		// stream.
		return c.readOnce(ctx, args)
	case err != nil:
		fmt.Printf("got err: %v\n", err)
		return nil, err
	}

	msg, err := parseXStreamSlice(result)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (c *Client) read(ctx context.Context, args *ReadArgs) (*Message, error) {
	result, err := c.readOnce(ctx, args)
	if result != nil || (err != nil && err != Empty) {
		return result, err
	}

	// If we're here, the queue was Empty. We only have more work to do if
	// args.Block is non-zero.
	if args.Block == 0 {
		return nil, Empty
	}

	// Wait for a message to be signaled on the notifications stream.
	ok, err := c.wait(ctx, args)
	if err != nil {
		return nil, err
	}
	if !ok {
		// No message was signaled within the block time.
		return nil, Empty
	}

	return c.readOnce(ctx, args)
}

func (c *Client) readOnce(ctx context.Context, args *ReadArgs) (*Message, error) {
	cmdKeys := []string{args.Name}
	cmdArgs := []any{int(c.ttl.Seconds()), args.Group, args.Consumer}
	result, err := readScript.Run(ctx, c.rdb, cmdKeys, cmdArgs...).Result()
	switch {
	case err == redis.Nil:
		return nil, Empty
	case err != nil:
		return nil, err
	}

	msg, err := parse(result)
	if err != nil {
		return nil, err
	}

	return msg, nil
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

func parseXStreamSlice(streams []redis.XStream) (*Message, error) {
	if len(streams) != 1 {
		return nil, fmt.Errorf("must have single stream, got %d", len(streams))
	}
	stream := streams[0]
	if len(stream.Messages) != 1 {
		return nil, fmt.Errorf("must have single message, got %d", len(stream.Messages))
	}
	message := stream.Messages[0]
	return &Message{
		Stream: stream.Stream,
		ID:     message.ID,
		Values: message.Values,
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
