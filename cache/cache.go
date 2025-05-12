// Package cache implements a typed cache which can serve stale data while
// fetching fresh data, and which takes a distributed lock before attempting a
// refresh in order to greatly reduce possible cache stampede effects.
//
// There is minimal support for multiple backends via NewCacheMultipleBackends.
// This is intended to be used for short durations to support migrating from one
// backend to another.  It acquires pessimistic locks for write operations, so
// performance will be worse.
//
// Data is stored in Redis, via the supplied Redis client.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/redis/go-redis/v9"
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

	// ErrDoesNotExist is returned if negative caching is enabled and the
	// non-existence of the specified key has been cached. It must also be
	// returned by cache fetchers when the specified key does not exist and
	// negative caching is wanted.
	ErrDoesNotExist = errors.New("requested item does not exist")

	// ErrDisallowedCacheValue is thrown if a client attempts to set an entry in
	// the cache to the zero value of the cache type T. This is disallowed to
	// prevent accidentally poisoning the cache with invalid data.
	ErrDisallowedCacheValue = errors.New("nil and zero values are not permitted")
)

type Fetcher[T any] func(ctx context.Context, key string) (T, error)

type Cache[T any] struct {
	name    string
	opts    cacheOptions
	clients []redis.Cmdable
	locker  lock.Locker
}

func NewCache[T any](
	client redis.Cmdable,
	name string,
	fresh, stale time.Duration,
	options ...Option,
) *Cache[T] {
	c := Cache[T]{
		name:    name,
		clients: []redis.Cmdable{client},
		locker:  lock.Locker{Clients: []redis.Cmdable{client}},
	}

	c.opts.Fresh = fresh
	c.opts.Stale = stale

	for _, o := range options {
		o.apply(&c.opts)
	}

	return &c
}

func NewCacheMultipleBackends[T any](
	clients []redis.Cmdable,
	name string,
	fresh, stale time.Duration,
	options ...Option,
) *Cache[T] {
	c := Cache[T]{
		name:    name,
		clients: clients,
		locker:  lock.Locker{Clients: clients},
	}

	c.opts.Fresh = fresh
	c.opts.Stale = stale

	for _, o := range options {
		o.apply(&c.opts)
	}

	return &c
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
	switch {
	case err == nil:
		return value, err
	case errors.Is(err, ErrDoesNotExist):
		// If we have cached nonexistence, we return that immediately and do no
		// other work.
		return value, err
	case errors.Is(err, errCacheMiss):
		// If it's a cache miss, we attempt to fill the cache.
		ctx, span := tracer.Start(
			ctx,
			"cache.miss",
			trace.WithAttributes(c.spanAttributes(key)...),
			trace.WithAttributes(attribute.String("cache.miss", "hard")),
		)
		defer span.End()
		return c.fill(ctx, key, fetcher)
	default:
		// For any other error, we fall back to fetching data from upstream.
		//
		// This is the only way we can avoid further amplifying load on the source
		// of the data (hidden behind the fetcher) when the cache isn't behaving.
		log.Warnw("cache fetch failed: falling back to direct fetch", "error", err)
		return fetcher(ctx, key)
	}
}

// Set updates the value stored in a given key with a provided object. This is
// not always needed (as usually values are fetched using the provided
// Fetcher[T]) but can be useful in some cases.
func (c *Cache[T]) Set(ctx context.Context, key string, value T) error {
	return c.set(ctx, key, value)
}

// fetch attempts to retrieve the value from cache. In the event of a hard cache
// miss it returns errCacheMiss, and for a soft miss it starts a goroutine to
// refill the cache.
func (c *Cache[T]) fetch(ctx context.Context, key string, fetcher Fetcher[T]) (value T, err error) {
	keys := c.keysFor(key)

	var fresh, data, negative any
	// return the first positive result
	for _, client := range c.clients {
		result, err := client.MGet(ctx, keys.fresh, keys.data, keys.negative).Result()
		if err != nil {
			return value, err
		}
		if len(result) != 3 {
			return value, fmt.Errorf("incorrect number of values from redis: got %d, expected 3", len(result))
		}

		fresh = result[0]
		data = result[1]
		negative = result[2]

		if fresh != nil && data != nil {
			// cache hit
			break
		}
	}

	if negative != nil {
		// cached non-existence
		return value, ErrDoesNotExist
	}

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
// and update the cache. It is called in the event of a cache miss.
func (c *Cache[T]) fill(ctx context.Context, key string, fetcher Fetcher[T]) (value T, err error) {
	log := logger.With(logging.GetFields(ctx)...).Sugar()
	span := trace.SpanFromContext(ctx)

	value, err = fetcher(ctx, key)
	if errors.Is(err, ErrDoesNotExist) {
		span.SetAttributes(attribute.String("cache.result", "negative (caching nonexistence)"))
		if cacheSetErr := c.setNegative(ctx, key); cacheSetErr != nil {
			// Errors encountered while filling the cache are not returned to the
			// caller: we don't want a cache availability problem to be exposed if the
			// value was already successfully fetched.
			recordError(ctx, cacheSetErr)
			log.Warnw("cache set negative failed", "error", cacheSetErr)
		}
		// we return the original err, so the caller can handle the
		// ErrDoesNotExist
		return value, err
	} else if err != nil {
		span.SetAttributes(attribute.String("cache.result", "error"))
		recordError(ctx, err)
		return value, err
	}

	span.SetAttributes(attribute.String("cache.result", "success (caching value)"))
	err = c.set(ctx, key, value)
	if err != nil {
		// Errors encountered while filling the cache are not returned to the
		// caller: we don't want a cache availability problem to be exposed if the
		// value was already successfully fetched.
		recordError(ctx, err)
		log.Warnw("cache fill failed", "error", err)
	}

	return value, nil
}

// setOnClient handles the entire transaction pipeline process for setting cache data on a single Redis client
func (c *Cache[T]) setOnClient(ctx context.Context, client redis.Cmdable, keys keys, data []byte) error {
	pipe := client.TxPipeline()
	// Remove any explicit nonexistence sentinel
	pipe.Del(ctx, keys.negative)
	// Update cached value
	pipe.Set(ctx, keys.data, string(data), c.opts.Stale)
	// Set freshness sentinel
	pipe.Set(ctx, keys.fresh, 1, c.opts.Fresh)

	_, err := pipe.Exec(ctx)
	return err
}

func (c *Cache[T]) set(ctx context.Context, key string, value T) error {
	// We don't accept the zero value of T into the cache. This could easily be a
	// bug and we don't want to take the risk of poisoning the cache.
	if reflect.ValueOf(value).IsZero() {
		return ErrDisallowedCacheValue
	}

	log := logger.With(logging.GetFields(ctx)...).Sugar()
	keys := c.keysFor(key)

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	l, err := c.acquireIfMultipleRedises(ctx, keys.lockMultiple, 5*time.Second)
	if err != nil {
		return err
	}
	defer func() {
		err := l.Release(ctx)
		if err != nil {
			recordError(ctx, fmt.Errorf("error releasing update lock: %w", err))
		}
	}()

	// Set in primary clients
	errs := []error{}
	for _, client := range c.clients {
		errs = append(errs, c.setOnClient(ctx, client, keys, data))
	}

	// Shadow write if configured
	if c.opts.ShadowWriteClient != nil {
		// Fire-and-forget shadow write
		go func() {
			shadowCtx, shadowSpan := tracer.Start(
				context.Background(),
				"cache.shadow_write",
				trace.WithAttributes(c.spanAttributes(key)...),
				trace.WithAttributes(attribute.String("cache.operation", "set")),
			)
			defer shadowSpan.End()

			log.Debugw("performing shadow write", "key", key)
			shadowErr := c.setOnClient(shadowCtx, c.opts.ShadowWriteClient, keys, data)
			if shadowErr != nil {
				recordError(shadowCtx, shadowErr)
				log.Warnw("shadow write failed", "key", key, "error", shadowErr, "operation", "set")
			}
		}()
	}

	return errors.Join(errs...)
}

func (c *Cache[T]) setNegative(ctx context.Context, key string) error {
	// If negative caching is not enabled, this is a no-op.
	if c.opts.Negative == 0 {
		return nil
	}

	log := logger.With(logging.GetFields(ctx)...).Sugar()
	keys := c.keysFor(key)

	// Record non-existence sentinel in the cache
	err := c.clients[0].Set(ctx, keys.negative, 1, c.opts.Negative).Err()

	// Shadow write if configured
	if c.opts.ShadowWriteClient != nil {
		// Fire-and-forget shadow write
		go func() {
			shadowCtx, shadowSpan := tracer.Start(
				context.Background(),
				"cache.shadow_write",
				trace.WithAttributes(c.spanAttributes(key)...),
				trace.WithAttributes(attribute.String("cache.operation", "setNegative")),
			)
			defer shadowSpan.End()

			log.Debugw("performing shadow negative write", "key", key)

			shadowErr := c.opts.ShadowWriteClient.Set(shadowCtx, keys.negative, 1, c.opts.Negative).Err()
			if shadowErr != nil {
				recordError(shadowCtx, shadowErr)
				log.Warnw("shadow negative write failed",
					"key", key,
					"error", shadowErr)
			}
		}()
	}

	return err
}

type _nullLock struct{}

var nullLock lock.Lock = &_nullLock{}

func (*_nullLock) Release(context.Context) error { return nil }

func (c *Cache[T]) acquireIfMultipleRedises(ctx context.Context, key string, ttl time.Duration) (lock.Lock, error) {
	if len(c.clients) == 1 {
		return nullLock, nil
	}
	return c.locker.Acquire(ctx, key, ttl)
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
	l, err := c.locker.TryAcquire(ctx, keys.lock, c.opts.Stale)
	if errors.Is(err, lock.ErrLockNotAcquired) {
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
			recordError(ctx, fmt.Errorf("error releasing update lock: %w", err))
		}
	}()

	// we can ignore the error here; c.fill() will record any
	// non-ErrDoesNotExist error for us already.
	_, _ = c.fill(ctx, key, fetcher)
}

type keys struct {
	data         string
	fresh        string
	lock         string
	lockMultiple string
	negative     string
}

func (c *Cache[T]) keysFor(key string) keys {
	return keys{
		data:         fmt.Sprintf("cache:data:%s:%s", c.name, key),
		fresh:        fmt.Sprintf("cache:fresh:%s:%s", c.name, key),
		lock:         fmt.Sprintf("cache:lock:%s:%s", c.name, key),
		lockMultiple: fmt.Sprintf("cache:lock-multiple:%s:%s", c.name, key),
		negative:     fmt.Sprintf("cache:negative:%s:%s", c.name, key),
	}
}

func (c *Cache[T]) spanAttributes(key string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("cache.name", c.name),
		attribute.String("cache.key", key),
	}
}

func recordError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	span.SetStatus(codes.Error, err.Error())
	sentry.CaptureException(err)
}
