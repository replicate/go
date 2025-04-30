package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type testObj struct {
	Value string `json:"value"`
}

func fetchTestObj(_ context.Context, key string) (testObj, error) {
	return testObj{
		Value: "value_for:" + key,
	}, nil
}

type mockWrapper struct {
	redismock.ClientMock

	name     string
	fresh    time.Duration
	stale    time.Duration
	negative time.Duration
}

func (m mockWrapper) ExpectCacheFetchEmpty(key string) {
	m.ExpectMGet(
		"cache:fresh:"+m.name+":"+key,
		"cache:data:"+m.name+":"+key,
		"cache:negative:"+m.name+":"+key,
	).SetVal([]any{nil, nil, nil})
}

func (m mockWrapper) ExpectCacheFetchErr(key string, err error) {
	m.ExpectMGet(
		"cache:fresh:"+m.name+":"+key,
		"cache:data:"+m.name+":"+key,
		"cache:negative:"+m.name+":"+key,
	).SetErr(err)
}

func (m mockWrapper) ExpectCacheFetchFresh(key string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	m.ExpectMGet(
		"cache:fresh:"+m.name+":"+key,
		"cache:data:"+m.name+":"+key,
		"cache:negative:"+m.name+":"+key,
	).SetVal([]any{1, string(data), nil})
}

func (m mockWrapper) ExpectCacheFetchNegative(key string) {
	m.ExpectMGet(
		"cache:fresh:"+m.name+":"+key,
		"cache:data:"+m.name+":"+key,
		"cache:negative:"+m.name+":"+key,
	).SetVal([]any{nil, nil, 1})
}

func (m mockWrapper) ExpectCacheFill(key string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	m.ExpectTxPipeline()
	m.ExpectDel("cache:negative:" + m.name + ":" + key).SetVal(0)
	m.ExpectSet("cache:data:"+m.name+":"+key, string(data), m.stale).SetVal("OK")
	m.ExpectSet("cache:fresh:"+m.name+":"+key, 1, m.fresh).SetVal("OK")
	m.ExpectTxPipelineExec()
}

func (m mockWrapper) ExpectCacheDelete(key string) {
	m.ExpectTxPipeline()
	m.ExpectDel("cache:negative:" + m.name + ":" + key).SetVal(0)
	m.ExpectDel("cache:data:" + m.name + ":" + key).SetVal(0)
	m.ExpectDel("cache:fresh:" + m.name + ":" + key).SetVal(0)
	m.ExpectTxPipelineExec()
}

func (m mockWrapper) ExpectCacheFillWithLock(key string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	lockMultiple := "cache:lock-multiple:" + m.name + ":" + key
	m.Regexp().ExpectSetNX(lockMultiple, `.*`, 5*time.Second).SetVal(true)
	m.ExpectTxPipeline()
	m.ExpectDel("cache:negative:" + m.name + ":" + key).SetVal(0)
	m.ExpectSet("cache:data:"+m.name+":"+key, string(data), m.stale).SetVal("OK")
	m.ExpectSet("cache:fresh:"+m.name+":"+key, 1, m.fresh).SetVal("OK")
	m.ExpectTxPipelineExec()
	m.Regexp().ExpectEvalSha(`.*`, []string{lockMultiple}, `.*`).SetVal(int64(1))
}

func (m mockWrapper) ExpectCacheFillWithLockErr(key string, err error) {
	lockMultiple := "cache:lock-multiple:" + m.name + ":" + key
	m.Regexp().ExpectSetNX(lockMultiple, `.*`, 5*time.Second).SetVal(true)
	m.ExpectTxPipeline()
	m.ExpectDel("cache:negative:" + m.name + ":" + key).SetErr(err)
	m.Regexp().ExpectEvalSha(`.*`, []string{lockMultiple}, `.*`).SetVal(int64(1))
}

func (m mockWrapper) ExpectCacheFillNegative(key string) {
	m.ExpectSet("cache:negative:"+m.name+":"+key, 1, m.negative).SetVal("OK")
}

// Helper function for polling mock expectations
func assertEventually(t *testing.T, condition func() error, msgAndArgs ...interface{}) {
	t.Helper()
	timeout := 100 * time.Millisecond // Adjust timeout as needed
	interval := 5 * time.Millisecond  // Adjust interval as needed

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-timer.C:
			assert.Fail(t, fmt.Sprintf("Condition not met within %v. Last error: %v", timeout, lastErr), msgAndArgs...)
			return
		case <-ticker.C:
			lastErr = condition()
			if lastErr == nil {
				return // Condition met
			}
		}
	}
}

func TestCacheFetchesWhenNotInCache(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCache[testObj](client, "objects", fresh, stale)

	obj := testObj{Value: "value_for:elephant"}

	cacheMock.ExpectCacheFetchEmpty("elephant")
	cacheMock.ExpectCacheFill("elephant", obj)

	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant", v.Value)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
}

func TestCacheReturnsValueFromCache(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCache[testObj](client, "objects", fresh, stale)

	obj := testObj{Value: "value_for:elephant_from_cache"}

	cacheMock.ExpectCacheFetchFresh("elephant", obj)

	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant_from_cache", v.Value)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
}

func TestMultipleCacheReturnsValueFromAnyCache(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client1, mock1 := redismock.NewClientMock()
	cacheMock1 := mockWrapper{
		ClientMock: mock1,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	client2, mock2 := redismock.NewClientMock()
	cacheMock2 := mockWrapper{
		ClientMock: mock2,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCacheMultipleBackends[testObj]([]redis.Cmdable{client1, client2}, "objects", fresh, stale)

	obj := testObj{Value: "value_for:elephant_from_cache"}

	cacheMock1.ExpectCacheFetchEmpty("elephant")
	cacheMock2.ExpectCacheFetchFresh("elephant", obj)

	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant_from_cache", v.Value)
	assert.NoError(t, cacheMock1.ExpectationsWereMet())
	assert.NoError(t, cacheMock2.ExpectationsWereMet())
}

func TestMultipleCacheReturnsValueFromFirstCacheHit(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client1, mock1 := redismock.NewClientMock()
	cacheMock1 := mockWrapper{
		ClientMock: mock1,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	client2, mock2 := redismock.NewClientMock()
	cacheMock2 := mockWrapper{
		ClientMock: mock2,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCacheMultipleBackends[testObj]([]redis.Cmdable{client1, client2}, "objects", fresh, stale)

	obj1 := testObj{Value: "value_for:elephant_from_cache1"}

	cacheMock1.ExpectCacheFetchFresh("elephant", obj1)
	// no expectations for cache2, it should not be touched

	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant_from_cache1", v.Value)
	assert.NoError(t, cacheMock1.ExpectationsWereMet())
	assert.NoError(t, cacheMock2.ExpectationsWereMet())
}

func TestMultipleCacheFreshOverridesNegative(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client1, mock1 := redismock.NewClientMock()
	cacheMock1 := mockWrapper{
		ClientMock: mock1,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	client2, mock2 := redismock.NewClientMock()
	cacheMock2 := mockWrapper{
		ClientMock: mock2,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCacheMultipleBackends[testObj]([]redis.Cmdable{client1, client2}, "objects", fresh, stale)

	obj := testObj{Value: "value_for:elephant_from_cache"}

	cacheMock1.ExpectCacheFetchNegative("elephant")
	cacheMock2.ExpectCacheFetchFresh("elephant", obj)

	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant_from_cache", v.Value)
	assert.NoError(t, cacheMock1.ExpectationsWereMet())
	assert.NoError(t, cacheMock2.ExpectationsWereMet())
}

// If non-existence is cached, we should get ErrDoesNotExist immediately from
// the cache.
func TestCacheReturnsDoesNotExistForNegativeCache(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second
	negative := 5 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,

		name:     "objects",
		fresh:    fresh,
		stale:    stale,
		negative: negative,
	}
	cache := NewCache[testObj](client, "objects", fresh, stale, WithNegativeCaching(negative))

	cacheMock.ExpectCacheFetchNegative("elephant")

	_, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.ErrorIs(t, err, ErrDoesNotExist)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
}

// If the fetcher returns ErrDoesNotExist and negative caching is configured, we
// store non-existence in the cache and return ErrDoesNotExist.
func TestCacheReturnsDoesNotExistForNegativeCacheWithFetch(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second
	negative := 5 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,

		name:     "objects",
		fresh:    fresh,
		stale:    stale,
		negative: negative,
	}
	cache := NewCache[testObj](client, "objects", fresh, stale, WithNegativeCaching(negative))

	cacheMock.ExpectCacheFetchEmpty("elephant")
	cacheMock.ExpectCacheFillNegative("elephant")

	_, err := cache.Get(ctx, "elephant", func(_ context.Context, _ string) (t testObj, err error) {
		return t, fmt.Errorf("not found: %w", ErrDoesNotExist)
	})

	assert.ErrorIs(t, err, ErrDoesNotExist)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
}

func TestCacheFetchesOnRedisError(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCache[testObj](client, "objects", fresh, stale)

	cacheMock.ExpectCacheFetchErr("elephant", errors.New("boom"))

	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant", v.Value)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
}

func TestCacheSet(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCache[testObj](client, "objects", fresh, stale)

	obj := testObj{Value: "value_for:elephant"}

	cacheMock.ExpectCacheFill("elephant", obj)

	err := cache.Set(ctx, "elephant", obj)

	assert.NoError(t, err)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
}

func TestCacheDelete(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCache[testObj](client, "objects", fresh, stale)

	cacheMock.ExpectCacheDelete("elephant")

	err := cache.Delete(ctx, "elephant")

	assert.NoError(t, err)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
}

func TestMultipleCacheSet(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client1, mock1 := redismock.NewClientMock()
	cacheMock1 := mockWrapper{
		ClientMock: mock1,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	client2, mock2 := redismock.NewClientMock()
	cacheMock2 := mockWrapper{
		ClientMock: mock2,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCacheMultipleBackends[testObj]([]redis.Cmdable{client1, client2}, "objects", fresh, stale)

	obj := testObj{Value: "value_for:elephant"}

	cacheMock1.ExpectCacheFillWithLock("elephant", obj)
	cacheMock2.ExpectCacheFillWithLock("elephant", obj)

	err := cache.Set(ctx, "elephant", obj)

	assert.NoError(t, err)
	assert.NoError(t, cacheMock1.ExpectationsWereMet())
	assert.NoError(t, cacheMock2.ExpectationsWereMet())
}

func TestMultipleCacheSetWritesToAllBackendsEvenWhenOneErrors(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client1, mock1 := redismock.NewClientMock()
	cacheMock1 := mockWrapper{
		ClientMock: mock1,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	client2, mock2 := redismock.NewClientMock()
	cacheMock2 := mockWrapper{
		ClientMock: mock2,

		name:  "objects",
		fresh: fresh,
		stale: stale,
	}
	cache := NewCacheMultipleBackends[testObj]([]redis.Cmdable{client1, client2}, "objects", fresh, stale)

	obj := testObj{Value: "value_for:elephant"}

	cacheMock1.ExpectCacheFillWithLockErr("elephant", errors.New("kaboom"))
	cacheMock2.ExpectCacheFillWithLock("elephant", obj)

	err := cache.Set(ctx, "elephant", obj)

	assert.ErrorContains(t, err, "kaboom")
	assert.NoError(t, cacheMock1.ExpectationsWereMet())
	assert.NoError(t, cacheMock2.ExpectationsWereMet())
}

func TestCacheSetNilForbidden(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, _ := redismock.NewClientMock()
	cache := NewCache[*testObj](client, "objects", fresh, stale)

	err := cache.Set(ctx, "elephant", nil)
	assert.ErrorIs(t, err, ErrDisallowedCacheValue)
}

func TestCacheSetZeroValueForbidden(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, _ := redismock.NewClientMock()
	cache := NewCache[testObj](client, "objects", fresh, stale)

	var value testObj

	err := cache.Set(ctx, "elephant", value)
	assert.ErrorIs(t, err, ErrDisallowedCacheValue)
}

func TestCacheWithShadowWriteClient(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	// Primary client
	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
	}

	// Shadow write client - allow out of order expectations for async operations
	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	// Create cache with shadow write client
	cache := NewCache[testObj](client, "objects", fresh, stale, WithShadowWriteClient(shadowClient))

	obj := testObj{Value: "value_for:elephant"}

	// Primary client handles normal flow
	cacheMock.ExpectCacheFetchEmpty("elephant")
	cacheMock.ExpectCacheFill("elephant", obj)

	// Shadow client only gets the write operations - no read expectations
	shadowMock.ExpectTxPipeline()
	shadowMock.ExpectDel("cache:negative:objects:elephant").SetVal(0)
	shadowMock.ExpectSet("cache:data:objects:elephant", `{"value":"value_for:elephant"}`, stale).SetVal("OK")
	shadowMock.ExpectSet("cache:fresh:objects:elephant", 1, fresh).SetVal("OK")
	shadowMock.ExpectTxPipelineExec()

	// Get should trigger a cache miss and fill both clients
	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant", v.Value)
	assert.NoError(t, cacheMock.ExpectationsWereMet())

	// Poll shadow mock expectations instead of sleeping
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for Get")
}

func TestCacheWithShadowWriteClientDirectSet(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	// Primary client
	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
	}

	// Shadow write client - allow out of order expectations for async operations
	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	// Create cache with shadow write client
	cache := NewCache[testObj](client, "objects", fresh, stale, WithShadowWriteClient(shadowClient))

	obj := testObj{Value: "direct_set_value"}

	// Primary client gets the write
	cacheMock.ExpectCacheFill("elephant", obj)

	// Shadow client also gets the write
	shadowMock.ExpectTxPipeline()
	shadowMock.ExpectDel("cache:negative:objects:elephant").SetVal(0)
	shadowMock.ExpectSet("cache:data:objects:elephant", `{"value":"direct_set_value"}`, stale).SetVal("OK")
	shadowMock.ExpectSet("cache:fresh:objects:elephant", 1, fresh).SetVal("OK")
	shadowMock.ExpectTxPipelineExec()

	// Direct Set should write to both clients
	err := cache.Set(ctx, "elephant", obj)

	assert.NoError(t, err)
	assert.NoError(t, cacheMock.ExpectationsWereMet())

	// Poll shadow mock expectations instead of sleeping
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for Set")
}

func TestCacheWithShadowWriteClientNegative(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second
	negative := 5 * time.Second

	// Primary client
	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
		negative:   negative,
	}

	// Shadow write client - allow out of order expectations for async operations
	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	// Create cache with shadow write client and negative caching
	cache := NewCache[testObj](client, "objects", fresh, stale,
		WithNegativeCaching(negative),
		WithShadowWriteClient(shadowClient))

	// Primary client handles the fetch and negative cache set
	cacheMock.ExpectCacheFetchEmpty("elephant")
	cacheMock.ExpectCacheFillNegative("elephant")

	// Shadow client only gets the negative write operation
	shadowMock.ExpectSet("cache:negative:objects:elephant", 1, negative).SetVal("OK")

	// Get with a fetcher that returns DoesNotExist
	_, err := cache.Get(ctx, "elephant", func(_ context.Context, _ string) (t testObj, err error) {
		return t, fmt.Errorf("not found: %w", ErrDoesNotExist)
	})

	assert.ErrorIs(t, err, ErrDoesNotExist)
	assert.NoError(t, cacheMock.ExpectationsWereMet())

	// Poll shadow mock expectations instead of sleeping
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for negative cache Get")
}

func TestCacheMultipleBackendsWithShadowWriteClient(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second

	// Primary clients
	client1, mock1 := redismock.NewClientMock()
	cacheMock1 := mockWrapper{
		ClientMock: mock1,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
	}

	client2, mock2 := redismock.NewClientMock()
	cacheMock2 := mockWrapper{
		ClientMock: mock2,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
	}

	// Shadow write client - allow out of order expectations for async operations
	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	// Create cache with shadow write client
	cache := NewCacheMultipleBackends[testObj](
		[]redis.Cmdable{client1, client2},
		"objects",
		fresh,
		stale,
		WithShadowWriteClient(shadowClient),
	)

	obj := testObj{Value: "value_for:elephant"}

	// Primary clients with lock handling for multiple backends
	cacheMock1.ExpectCacheFillWithLock("elephant", obj)
	cacheMock2.ExpectCacheFillWithLock("elephant", obj)

	// Shadow client gets the writes but no locking logic
	shadowMock.ExpectTxPipeline()
	shadowMock.ExpectDel("cache:negative:objects:elephant").SetVal(0)
	shadowMock.ExpectSet("cache:data:objects:elephant", `{"value":"value_for:elephant"}`, stale).SetVal("OK")
	shadowMock.ExpectSet("cache:fresh:objects:elephant", 1, fresh).SetVal("OK")
	shadowMock.ExpectTxPipelineExec()

	err := cache.Set(ctx, "elephant", obj)

	assert.NoError(t, err)
	assert.NoError(t, cacheMock1.ExpectationsWereMet())
	assert.NoError(t, cacheMock2.ExpectationsWereMet())

	// Poll shadow mock expectations instead of sleeping
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for multiple backend Set")
}

func TestCacheOperationsSucceedWhenShadowClientFails(t *testing.T) {
	ctx := context.Background()

	fresh := 10 * time.Second
	stale := 30 * time.Second
	negative := 5 * time.Second

	// Primary client that works
	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
		negative:   negative,
	}

	// Shadow write client that fails - we don't use expectations on it in this test
	shadowClient, _ := redismock.NewClientMock()

	// Set all the primary client's expectations upfront to avoid races
	cacheMock.ExpectCacheFill("elephant", testObj{Value: "shadow_failure_test"})
	cacheMock.ExpectCacheFetchEmpty("elephant_get")
	cacheMock.ExpectCacheFill("elephant_get", testObj{Value: "value_for:elephant_get"})
	cacheMock.ExpectCacheFetchEmpty("missing")
	cacheMock.ExpectCacheFillNegative("missing")

	// Create cache with shadow write client
	cache := NewCache[testObj](client, "objects", fresh, stale,
		WithNegativeCaching(negative),
		WithShadowWriteClient(shadowClient))

	// Test 1: Direct set succeeds despite shadow client failure
	obj := testObj{Value: "shadow_failure_test"}

	// Set should still succeed because shadow writes are fire-and-forget
	err := cache.Set(ctx, "elephant", obj)
	assert.NoError(t, err)

	// Test 2: Get succeeds and updates cache despite shadow client failure during fill
	// Get should succeed despite shadow client errors
	v, err := cache.Get(ctx, "elephant_get", fetchTestObj)
	assert.NoError(t, err)
	assert.Equal(t, "value_for:elephant_get", v.Value)

	// Test 3: Negative caching works even if shadow client fails
	// Get with DoesNotExist should still succeed in setting negative cache
	_, err = cache.Get(ctx, "missing", func(_ context.Context, _ string) (t testObj, err error) {
		return t, fmt.Errorf("not found: %w", ErrDoesNotExist)
	})

	assert.ErrorIs(t, err, ErrDoesNotExist)

	// All operations on primary cache should have completed successfully
	assert.NoError(t, cacheMock.ExpectationsWereMet())

	// We don't check shadow expectations since we're intentionally making the shadow client fail
}
