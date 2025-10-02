// Package queue provides a queue implementation based on Redis streams which
// uses [shuffle-sharding] to provide a degree of isolation between queue
// tenants.
//
// Shuffle sharding is a technique that divides a shared resource into N virtual
// resources, and randomly but consistently assigned some M (< N) of those
// virtual resources to each tenant as its "shard." Only if two tenants share
// the exact same shard are their fates fully entangled. For reasonably modest
// values of M and N, the probability of two tenants sharing a shard can be made
// exceedingly small: 1 / (N choose M).
//
// In our implementation, we divide a single logical queue into N "virtual
// queues," each of which is a separate Redis stream. Each tenant is then
// assigned M of these virtual queues as their "shard" based on a tenant key.
// When enqueueing messages, we check the lengths of each stream in the tenant's
// shard and enqueue to the shortest one. When dequeuing messages, consumers
// cycle through all the streams in turn.
//
// In practice, this means that tenants making heavy use of the queue are
// straightforwardly avoided by other tenants, so long as those tenants' shards
// do not overlap fully with the heavy user's shard.
//
// One wrinkle is that it is not possible using the Redis stream abstraction to
// block and wait for a single message across multiple streams. XREADGROUP can
// do this, but if more than one stream has messages waiting at the moment the
// call is made, it will claim and return one message per stream, which is not
// suitable for our use case. To work around this, we use a separate
// "notifications" stream to signal to consumers that work has been added to the
// queue. Triggered by this stream, they can then read from the streams that
// make up the queue.
//
// [shuffle-sharding]: https://aws.amazon.com/builders-library/workload-isolation-using-shuffle-sharding/
package queue

import (
	"context"
	_ "embed" // to provide go:embed support
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed len.lua
	lenCmd    string
	lenScript = redis.NewScript(lenCmd)

	//go:embed pendingcount.lua
	pendingCountCmd    string
	pendingCountScript = redis.NewScript(pendingCountCmd)

	//go:embed stats.lua
	statsCmd    string
	statsScript = redis.NewScript(statsCmd)

	//go:embed read.lua
	readCmd    string
	readScript = redis.NewScript(readCmd)

	//go:embed write.lua
	writeCmd    string
	writeScript = redis.NewScript(writeCmd)

	//go:embed writetracking.lua
	writeTrackingCmd    string
	writeTrackingScript = redis.NewScript(
		strings.ReplaceAll(
			writeTrackingCmd,
			"__META_CANCELATION_HASH__",
			MetaCancelationHash,
		),
	)
)

const (
	MetaCancelationHash = "meta:cancelation"

	metaCancelationGCBatchSize = 100
)

func prepare(ctx context.Context, rdb redis.Cmdable) error {
	if err := lenScript.Load(ctx, rdb).Err(); err != nil {
		return err
	}
	if err := pendingCountScript.Load(ctx, rdb).Err(); err != nil {
		return err
	}
	if err := statsScript.Load(ctx, rdb).Err(); err != nil {
		return err
	}
	if err := readScript.Load(ctx, rdb).Err(); err != nil {
		return err
	}
	if err := writeScript.Load(ctx, rdb).Err(); err != nil {
		return err
	}
	if err := writeTrackingScript.Load(ctx, rdb).Err(); err != nil {
		return err
	}
	return nil
}

func gcMetaCancelation(ctx context.Context, rdb redis.Cmdable) (int, error) {
	now := time.Now().UTC().Unix()
	keysToDelete := []string{}
	iter := rdb.HScan(ctx, MetaCancelationHash, 0, "*:expiry:*", 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()

		keyParts := strings.Split(key, ":")
		if len(keyParts) != 3 {
			continue
		}

		keyTime, err := strconv.ParseInt(keyParts[2], 0, 64)
		if err != nil {
			continue
		}

		if keyTime > now {
			keysToDelete = append(keysToDelete, key, keyParts[0])
		}
	}

	if err := iter.Err(); err != nil {
		return 0, err
	}

	for i := 0; i < len(keysToDelete); i += metaCancelationGCBatchSize {
		sliceEnd := i + metaCancelationGCBatchSize
		if sliceEnd > len(keysToDelete) {
			sliceEnd = len(keysToDelete)
		}

		if err := rdb.HDel(
			ctx,
			MetaCancelationHash,
			keysToDelete[i:sliceEnd]...,
		).Err(); err != nil {
			return 0, err
		}
	}

	return len(keysToDelete), nil
}
