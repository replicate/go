package queue

import (
	"time"
)

type WriteArgs struct {
	Name   string         // queue name
	Values map[string]any // message values

	Streams         int    // total number of streams
	StreamsPerShard int    // number of streams in each shard
	ShardKey        []byte // tenant key to determine shard
}

type ReadArgs struct {
	Name     string        // queue name
	Group    string        // consumer group name
	Consumer string        // consumer ID
	Block    time.Duration // total blocking time
}

type Message struct {
	Stream string // stream from which this message was read
	ID     string
	Values map[string]any
}
