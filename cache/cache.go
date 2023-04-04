// Package cache implements a typed cache which can serve stale data while
// fetching fresh data, and which takes a distributed lock before attempting a
// refresh in order to greatly reduce possible cache stampede effects.
//
// Data is stored in Redis, via the supplied Redis client.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/replicate/go/lock"
	"github.com/replicate/go/logging"
	"github.com/replicate/go/telemetry"
)

var (
	logger = logging.New("cache")
	tracer = telemetry.Tracer("go", "cache")

	// internal error indicating a hard cache miss
	errCacheMiss = errors.New("value not in cache")
)

type Fetcher[T any] func(ctx context.Context, key string) (T, error)

type Cache[T any] struct {
	name  string
	fresh time.Duration
	stale time.Duration

	client redis.Cmdable
	locker lock.Locker
}

func NewCache[T any](
	client redis.Cmdable,
	name string,
	fresh time.Duration,
	stale time.Duration,
) *Cache[T] {
	return &Cache[T]{
		name:  name,
		fresh: fresh,
		stale: stale,

		client: client,
		locker: lock.Locker{Client: client},
	}
}

func (c *Cache[T]) Prepare(ctx context.Context) error {
	log := logger.Sugar()
	if c == nil {
		log.Warnf("cache not configured: prepare is a no-op")
		return nil
	}
	return c.locker.Prepare(ctx)
}

// Get fetches an item with the given key from cache. In the event of a cache
// miss or an error communicating with the cache, it will fall back to fetching
// the item from source using the passed fetcher.
func (c *Cache[T]) Get(ctx context.Context, key string, fetcher Fetcher[T]) (value T, err error) {
	log := logger.With(logging.GetFields(ctx)...).Sugar()

	if c == nil {
		log.Warnf("cache not configured: fetching data directly")
		return fetcher(ctx, key)
	}

	value, err = c.fetch(ctx, key, fetcher)
	if err != nil {
		// If fetching from cache threw any error other than a cache miss, we
		// immediately fall back to fetching data from upstream.
		//
		// This is the only way we can avoid further amplifying load on the source
		// of the data (hidden behind the fetcher) when the cache isn't behaving.
		if err != errCacheMiss {
			log.Warnw("cache fetch failed: falling back to direct fetch", "error", err)
			return fetcher(ctx, key)
		}

		// Otherwise, it's a cache miss.
		value, err = c.fill(ctx, key, fetcher)
	}

	return value, err
}

// fetch attempts to retrieve the value from cache. In the event of a hard cache
// miss it returns errCacheMiss, and for a soft miss it starts a goroutine to
// refill the cache.
func (c *Cache[T]) fetch(ctx context.Context, key string, fetcher Fetcher[T]) (value T, err error) {
	keys := c.keysFor(key)

	result, err := c.client.MGet(ctx, keys.fresh, keys.data).Result()
	if err != nil {
		return value, err
	}
	if len(result) != 2 {
		return value, fmt.Errorf("incorrect number of values from redis: got %d, expected 2", len(result))
	}

	fresh := result[0]
	data := result[1]

	if data == nil {
		// hard cache miss
		return value, errCacheMiss
	}

	if fresh == nil {
		// soft cache miss: kick off a refresh
		c.refresh(ctx, key, fetcher)
	}

	valueStr, ok := data.(string)
	if !ok {
		return value, fmt.Errorf("unable to interpret redis value as string: %v", data)
	}

	err = json.Unmarshal([]byte(valueStr), &value)
	if err != nil {
		return value, err
	}

	return value, nil
}

// fill attempts to fetch a value from the upstream (using the passed fetcher)
// and update the cache. It is called in the event of a hard cache miss.
func (c *Cache[T]) fill(ctx context.Context, key string, fetcher Fetcher[T]) (value T, err error) {
	log := logger.With(logging.GetFields(ctx)...).Sugar()

	ctx, span := tracer.Start(
		ctx,
		"cache.miss",
		trace.WithAttributes(c.spanAttributes(key)...),
		trace.WithAttributes(attribute.String("cache.miss", "hard")),
	)
	defer span.End()

	value, err = fetcher(ctx, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return value, err
	}

	err = c.set(ctx, key, value)
	if err != nil {
		// Errors encountered while filling the cache are not returned to the
		// caller: we don't want a cache availability problem to be exposed if the
		// value was already successfully fetched.
		span.SetStatus(codes.Error, err.Error())
		log.Warnw("cache fill failed", "error", err)
	}

	return value, nil
}

func (c *Cache[T]) set(ctx context.Context, key string, value T) error {
	keys := c.keysFor(key)

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	// Update cached value
	_, err = c.client.Set(ctx, keys.data, string(data), c.stale).Result()
	if err != nil {
		return err
	}

	// Set freshness sentinel
	_, err = c.client.Set(ctx, keys.fresh, 1, c.fresh).Result()
	if err != nil {
		return err
	}

	return nil
}

// refresh attempts to refill the cache in the event of a soft cache miss. We
// attempt to acquire a shared lock on the cache key, and if successful we fetch
// the value and update the cache in a goroutine. If we fail to acquire the lock
// then we do nothing, on the assumption that someone else is refilling the
// cache.
func (c *Cache[T]) refresh(ctx context.Context, key string, fetcher Fetcher[T]) {
	keys := c.keysFor(key)

	// We acquire the lock for (at most) the duration for which we're prepared to
	// serve stale values.
	l, err := c.locker.TryAcquire(ctx, keys.lock, c.stale)
	if err == lock.ErrLockNotAcquired {
		return
	} else if err != nil {
		// We record other errors but don't do anything to interrupt serving from
		// stale data.
		sentry.CaptureException(fmt.Errorf("error acquiring cache lock: %w", err))
		return
	}

	// Create a new root span which links to the span which triggered the
	// background refresh.
	//
	// Note: it's up to refreshInner to ensure that End() is called on this span!
	link := trace.LinkFromContext(ctx)
	ctx, _ = tracer.Start(
		context.Background(),
		"cache.miss",
		trace.WithLinks(link),
		trace.WithAttributes(c.spanAttributes(key)...),
		trace.WithAttributes(attribute.String("cache.miss", "soft")),
	)
	go c.refreshInner(ctx, key, fetcher, l)
}

func (c *Cache[T]) refreshInner(ctx context.Context, key string, fetcher Fetcher[T], l lock.Lock) {
	span := trace.SpanFromContext(ctx)

	defer span.End()
	defer func() {
		err := l.Release(ctx)
		if err != nil {
			handleRefreshError(ctx, fmt.Errorf("error releasing update lock: %w", err))
		}
	}()

	value, err := fetcher(ctx, key)
	if err != nil {
		handleRefreshError(ctx, fmt.Errorf("error fetching fresh value for cache: %w", err))
		return
	}
	err = c.set(ctx, key, value)
	if err != nil {
		handleRefreshError(ctx, fmt.Errorf("error updating cache: %w", err))
		return
	}
}

type keys struct {
	data  string
	fresh string
	lock  string
}

func (c *Cache[T]) keysFor(key string) keys {
	return keys{
		data:  fmt.Sprintf("cache:data:%s:%s", c.name, key),
		fresh: fmt.Sprintf("cache:fresh:%s:%s", c.name, key),
		lock:  fmt.Sprintf("cache:lock:%s:%s", c.name, key),
	}
}

func (c *Cache[T]) spanAttributes(key string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("cache.name", c.name),
		attribute.String("cache.key", key),
	}
}

func handleRefreshError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	span.SetStatus(codes.Error, err.Error())
	sentry.CaptureException(err)
}
