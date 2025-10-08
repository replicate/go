package queue

import (
	"time"
)

type queueError string

func (e queueError) Error() string {
	return string(e)
}

// Empty is the sentinel error returned from read calls when no messages are
// available.
const Empty = queueError("queue: empty")

type WriteArgs struct {
	Name     string         // queue name
	Values   map[string]any // message values
	Deadline time.Time      // time after which message will be cancel (only when tracked)

	Streams         int    // total number of streams
	StreamsPerShard int    // number of streams in each shard
	ShardKey        []byte // tenant key to determine shard
}

type ReadArgs struct {
	Name         string        // queue name
	Group        string        // consumer group name
	Consumer     string        // consumer ID
	PreferStream string        // if specified, prefer reading from this stream
	Block        time.Duration // total blocking time
}

type Message struct {
	Stream string // stream from which this message was read
	ID     string
	Values map[string]any
}
