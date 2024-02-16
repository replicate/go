package cache

import "time"

type Option interface {
	apply(*cacheOptions)
}

type cacheOptions struct {
	Fresh    time.Duration
	Stale    time.Duration
	Negative time.Duration
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
