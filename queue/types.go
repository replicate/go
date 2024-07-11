package queue

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb redis.Cmdable

	// Queue TTL
	ttl time.Duration
}

type WriteArgs struct {
	Name string

	// Shuffle sharding configuration
	Streams         int
	StreamsPerShard int
	ShardKey        []byte

	// Passed through to [redis.XAddArgs].
	//
	// TODO: Add support for the other XADD options.
	Values map[string]any
}

type ReadArgs struct {
	Name string

	// Passed through to [redis.XReadGroupArgs]
	Group    string
	Consumer string
	Block    time.Duration
	NoAck    bool
}
