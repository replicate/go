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
	Name   string
	Values map[string]any

	// Shuffle sharding configuration
	Streams         int
	StreamsPerShard int
	ShardKey        []byte
}

type ReadArgs struct {
	Name     string
	Group    string
	Consumer string
	Block    time.Duration // total blocking time
}

type Message struct {
	Stream string
	ID     string
	Values map[string]any
}
