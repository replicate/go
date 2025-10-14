package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/multierr"

	"github.com/replicate/go/shuffleshard"
)

var (
	ErrInvalidReadArgs           = errors.New("queue: invalid read arguments")
	ErrInvalidWriteArgs          = errors.New("queue: invalid write arguments")
	ErrNoMatchingMessageInStream = errors.New("queue: no matching message in stream")
	ErrInvalidMetaCancelation    = errors.New("queue: invalid meta cancelation")
	ErrInvalidNTimeDigits        = errors.New("queue: invalid number of timestamp digits")
	ErrStopGC                    = errors.New("queue: stop garbage collection")

	streamSuffixPattern = regexp.MustCompile(`\A:s(\d+)\z`)
)

const (
	metaCancelationGCBatchSize = 100
)

type Client struct {
	rdb redis.Cmdable
	ttl time.Duration // ttl for all keys in queue

	trackField string
}

type Stats struct {
	// Len is the aggregate length of the queue, as reported by XLEN
	Len int64
	// PendingCount is the aggregate count of pending entries, as reported by XPENDING
	PendingCount int64
}

func NewClient(rdb redis.Cmdable, ttl time.Duration) *Client {
	return &Client{rdb: rdb, ttl: ttl}
}

func NewTrackingClient(rdb redis.Cmdable, ttl time.Duration, field string) *Client {
	return &Client{rdb: rdb, ttl: ttl, trackField: field}
}

// Prepare stores the write and read scripts in the Redis script cache so that
// they can be more efficiently called with EVALSHA.
func (c *Client) Prepare(ctx context.Context) error {
	return prepare(ctx, c.rdb)
}

// OnGCFunc is called periodically during GC *before* deleting the expired keys. The
// argument given is the "track values" as extracted from the meta cancelation key.
type OnGCFunc func(ctx context.Context, trackValues []string) error

// GC performs all garbage collection operations that cannot be automatically performed
// via key expiry, which is the "meta:cancelation" hash at the time of this writing. The
// nTimeDigits argument is used to construct the key match to include that many digits of
// of the current server time as a way of limiting the keyspace scanned. As a special
// case, any value <= -1 will result in all keys being scanned.
func (c *Client) GC(ctx context.Context, nTimeDigits int, f OnGCFunc) (uint64, uint64, error) {
	now, err := c.rdb.Time(ctx).Result()
	if err != nil {
		return 0, 0, err
	}

	nowUnix := now.Unix()

	nonFatalErrors := []error{}

	idsToDelete := []string{}
	keysToDelete := []string{}

	match := "*:expiry:*"
	if nTimeDigits > -1 {
		nowUnixString := strconv.Itoa(int(nowUnix))

		if nTimeDigits > len(nowUnixString) {
			return 0, 0, ErrInvalidNTimeDigits
		}

		match = fmt.Sprintf("*:expiry:%s*", nowUnixString[:nTimeDigits])
	}

	iter := c.rdb.HScanNoValues(ctx, MetaCancelationHash, 0, match, 0).Iterator()
	total := uint64(0)
	twiceDeleted := uint64(0)

	for iter.Next(ctx) {
		key := iter.Val()
		total++

		if len(idsToDelete) >= metaCancelationGCBatchSize {
			n, err := c.gcProcessBatch(ctx, f, idsToDelete, keysToDelete)
			if err != nil {
				if errors.Is(err, ErrStopGC) {
					return total, twiceDeleted / 2, err
				}

				nonFatalErrors = append(nonFatalErrors, err)
			}

			twiceDeleted += uint64(n)

			idsToDelete = []string{}
			keysToDelete = []string{}

			now, err = c.rdb.Time(ctx).Result()
			if err != nil {
				return total, twiceDeleted / 2, err
			}

			nowUnix = now.Unix()
		}

		keyParts := strings.Split(key, ":")
		if len(keyParts) != 3 {
			continue
		}

		keyTime, err := strconv.ParseInt(keyParts[2], 0, 64)
		if err != nil {
			nonFatalErrors = append(nonFatalErrors, err)
			continue
		}

		if nowUnix > keyTime {
			keysToDelete = append(keysToDelete, key, keyParts[0])
			idsToDelete = append(idsToDelete, keyParts[0])
		}
	}

	n, err := c.gcProcessBatch(ctx, f, idsToDelete, keysToDelete)
	if err != nil {
		if errors.Is(err, ErrStopGC) {
			return total, twiceDeleted / 2, err
		}

		nonFatalErrors = append(nonFatalErrors, err)
	}

	twiceDeleted += uint64(n)

	if err := iter.Err(); err != nil {
		return total, twiceDeleted / 2, err
	}

	return total, twiceDeleted / 2, multierr.Combine(nonFatalErrors...)
}

func (c *Client) gcProcessBatch(ctx context.Context, f OnGCFunc, idsToDelete, keysToDelete []string) (int64, error) {
	if len(idsToDelete) == 0 || len(keysToDelete) == 0 {
		return 0, nil
	}

	if err := c.callOnGC(ctx, f, idsToDelete); err != nil {
		// NOTE: The client `OnGCFunc` may request interruption via the `ErrStopGC`
		// error as a way to prevent the `HDel`.
		if errors.Is(err, ErrStopGC) {
			return 0, err
		}
	}

	nDeleted, err := c.rdb.HDel(ctx, MetaCancelationHash, keysToDelete...).Result()
	if err != nil {
		return nDeleted, err
	}

	// NOTE: ZRem requires an explicit []any which cannot be automatically
	// converted from a []string.
	zremArgs := make([]any, len(idsToDelete))
	for i, id := range idsToDelete {
		zremArgs[i] = id
	}

	if err := c.rdb.ZRem(ctx, MetaDeadlinesZSet, zremArgs...).Err(); err != nil {
		return nDeleted, err
	}

	return nDeleted, nil
}

func (c *Client) callOnGC(ctx context.Context, f OnGCFunc, idsToDelete []string) error {
	if f == nil {
		return nil
	}

	pipe := c.rdb.Pipeline()
	hValCmds := make([]*redis.StringCmd, len(idsToDelete))

	for i, idToDelete := range idsToDelete {
		hValCmds[i] = pipe.HGet(ctx, MetaCancelationHash, idToDelete)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	trackValues := make([]string, len(idsToDelete))

	for i, hValCmd := range hValCmds {
		msgBytes, err := hValCmd.Bytes()
		if err != nil {
			return err
		}

		msg := &metaCancelation{}
		if err := json.Unmarshal(msgBytes, msg); err != nil {
			return err
		}

		trackValues[i] = msg.TrackValue
	}

	return f(ctx, trackValues)
}

// DeadlineExceeded returns a slice of "track values" that have exceeded their deadline
// within a given duration into the past. The times are truncated to the second because
// the deadlines scored set uses unix timestamps as scores.
func (c *Client) DeadlineExceeded(ctx context.Context, within time.Duration) ([]string, error) {
	start, err := c.rdb.Time(ctx).Result()
	if err != nil {
		return []string{}, err
	}

	return c.rdb.ZRangeByScore(
		ctx,
		MetaDeadlinesZSet,
		&redis.ZRangeBy{
			Min: strconv.Itoa(int(start.Add(-within).Unix())),
			Max: strconv.Itoa(int(start.Add(1 * time.Second).Unix())),
		},
	).Result()
}

// Len calculates the aggregate length (XLEN) of the queue. It adds up the
// lengths of all the streams in the queue.
func (c *Client) Len(ctx context.Context, name string) (int64, error) {
	return lenScript.RunRO(ctx, c.rdb, []string{name}).Int64()
}

// Stats calculates aggregate statistics about the queue and consumer group.
func (c *Client) Stats(ctx context.Context, queue string, group string) (Stats, error) {
	out, err := statsScript.RunRO(ctx, c.rdb, []string{queue}, group).Int64Slice()
	if err != nil {
		return Stats{}, err
	}
	return Stats{Len: out[0], PendingCount: out[1]}, nil
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

	cmdArgs = append(
		cmdArgs,
		int(c.ttl.Seconds()),
		args.Streams,
		len(shard),
	)

	if c.trackField != "" {
		deadlineUnix := int64(0)
		if !args.Deadline.IsZero() {
			deadlineUnix = args.Deadline.Unix()
		}

		cmdArgs = append(
			cmdArgs,
			c.trackField,
			// NOTE: Deadline is an optional field in WriteArgs, so the Unix value may be
			// passed as zero so that the writeTrackingScript uses a default value of the
			// server time + ttl.
			deadlineUnix,
		)
	}

	for _, s := range shard {
		cmdArgs = append(cmdArgs, s)
	}
	for k, v := range args.Values {
		cmdArgs = append(cmdArgs, k, v)
	}

	if c.trackField != "" {
		return writeTrackingScript.Run(ctx, c.rdb, cmdKeys, cmdArgs...).Text()
	}

	return writeScript.Run(ctx, c.rdb, cmdKeys, cmdArgs...).Text()
}

type metaCancelation struct {
	StreamID   string `json:"stream_id"`
	MsgID      string `json:"msg_id"`
	TrackValue string `json:"track_value"`
	Deadline   int64  `json:"deadline"`
}

// Del supports removal of a message when the given `fieldValue` matches a "meta
// cancelation" key as written when using a client with tracking support.
func (c *Client) Del(ctx context.Context, fieldValue string) error {
	msgBytes, err := c.rdb.HGet(ctx, MetaCancelationHash, fieldValue).Bytes()
	if err != nil {
		return err
	}

	msg := &metaCancelation{}
	if err := json.Unmarshal(msgBytes, msg); err != nil {
		return err
	}

	if msg.StreamID == "" {
		return fmt.Errorf("empty stream_id: %w", ErrInvalidMetaCancelation)
	}

	if msg.MsgID == "" {
		return fmt.Errorf("empty msg_id: %w", ErrInvalidMetaCancelation)
	}

	n, err := c.rdb.XDel(ctx, msg.StreamID, msg.MsgID).Result()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf(
			"field-value=%q stream=%q message-id=%q: %w",
			fieldValue,
			msg.StreamID,
			msg.MsgID,
			ErrNoMatchingMessageInStream,
		)
	}

	return nil
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
