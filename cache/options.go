package cache

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type Option interface {
	apply(*cacheOptions)
}

type cacheOptions struct {
	Fresh             time.Duration
	Stale             time.Duration
	Negative          time.Duration
	ShadowWriteClient redis.Cmdable
}

type optionFunc func(*cacheOptions)

func (fn optionFunc) apply(opts *cacheOptions) {
	fn(opts)
}

// WithNegativeCaching configures the cache to allow caching of a negative
// ("does not exist") result for up to the specified duration.
func WithNegativeCaching(duration time.Duration) Option {
	return optionFunc(func(opts *cacheOptions) {
		opts.Negative = duration
	})
}

// WithShadowWrite configures the cache to use a separate Redis client for
// shadow writes. This is useful for writing to a different Redis instance
// than the one used for reading. This is useful for write-through caching
func WithShadowWriteClient(client redis.Cmdable) Option {
	return optionFunc(func(opts *cacheOptions) {
		opts.ShadowWriteClient = client
	})
}
