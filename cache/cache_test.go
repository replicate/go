package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/go/test"
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
	m.ExpectDel("cache:data:"+m.name+":"+key, "cache:fresh:"+m.name+":"+key).SetVal(0)
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

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
	}

	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	cache := NewCache[testObj](client, "objects", fresh, stale, WithShadowWriteClient(shadowClient, 1*time.Second))

	obj := testObj{Value: "value_for:elephant"}

	// Expectations for primary client
	cacheMock.ExpectCacheFetchEmpty("elephant")
	cacheMock.ExpectCacheFill("elephant", obj)

	// Expectations for shadow client (write only)
	shadowMock.ExpectTxPipeline()
	shadowMock.ExpectDel("cache:negative:objects:elephant").SetVal(0)
	shadowMock.ExpectSet("cache:data:objects:elephant", `{"value":"value_for:elephant"}`, stale).SetVal("OK")
	shadowMock.ExpectSet("cache:fresh:objects:elephant", 1, fresh).SetVal("OK")
	shadowMock.ExpectTxPipelineExec()

	// Trigger Get -> cache miss -> fill
	v, err := cache.Get(ctx, "elephant", fetchTestObj)

	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "value_for:elephant", v.Value)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for Get")
}

func TestCacheWithShadowWriteClientDirectSet(t *testing.T) {
	ctx := context.Background()
	fresh := 10 * time.Second
	stale := 30 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
	}

	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	cache := NewCache[testObj](client, "objects", fresh, stale, WithShadowWriteClient(shadowClient, 1*time.Second))

	obj := testObj{Value: "direct_set_value"}

	// Expectations for primary client
	cacheMock.ExpectCacheFill("elephant", obj)

	// Expectations for shadow client
	shadowMock.ExpectTxPipeline()
	shadowMock.ExpectDel("cache:negative:objects:elephant").SetVal(0)
	shadowMock.ExpectSet("cache:data:objects:elephant", `{"value":"direct_set_value"}`, stale).SetVal("OK")
	shadowMock.ExpectSet("cache:fresh:objects:elephant", 1, fresh).SetVal("OK")
	shadowMock.ExpectTxPipelineExec()

	// Trigger direct Set
	err := cache.Set(ctx, "elephant", obj)

	assert.NoError(t, err)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for Set")
}

func TestCacheWithShadowWriteClientNegative(t *testing.T) {
	ctx := context.Background()
	fresh := 10 * time.Second
	stale := 30 * time.Second
	negative := 5 * time.Second

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
		negative:   negative,
	}

	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	cache := NewCache[testObj](client, "objects", fresh, stale,
		WithNegativeCaching(negative),
		WithShadowWriteClient(shadowClient, 1*time.Second))

	// Expectations for primary client
	cacheMock.ExpectCacheFetchEmpty("elephant")
	cacheMock.ExpectCacheFillNegative("elephant")

	// Expectations for shadow client (evict)
	shadowMock.ExpectDel("cache:data:objects:elephant", "cache:fresh:objects:elephant").SetVal(0)
	shadowMock.ExpectSet("cache:negative:objects:elephant", 1, negative).SetVal("OK")

	// Trigger Get with fetcher returning ErrDoesNotExist
	_, err := cache.Get(ctx, "elephant", func(_ context.Context, _ string) (t testObj, err error) {
		return t, fmt.Errorf("not found: %w", ErrDoesNotExist)
	})

	assert.ErrorIs(t, err, ErrDoesNotExist)
	assert.NoError(t, cacheMock.ExpectationsWereMet())
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for negative cache Get")
}

func TestCacheMultipleBackendsWithShadowWriteClient(t *testing.T) {
	ctx := context.Background()
	fresh := 10 * time.Second
	stale := 30 * time.Second

	client1, mock1 := redismock.NewClientMock()
	cacheMock1 := mockWrapper{ClientMock: mock1, name: "objects", fresh: fresh, stale: stale}
	client2, mock2 := redismock.NewClientMock()
	cacheMock2 := mockWrapper{ClientMock: mock2, name: "objects", fresh: fresh, stale: stale}

	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	cache := NewCacheMultipleBackends[testObj](
		[]redis.Cmdable{client1, client2},
		"objects",
		fresh,
		stale,
		WithShadowWriteClient(shadowClient, 1*time.Second),
	)

	obj := testObj{Value: "value_for:elephant"}

	// Expectations for primary clients (with locking)
	cacheMock1.ExpectCacheFillWithLock("elephant", obj)
	cacheMock2.ExpectCacheFillWithLock("elephant", obj)

	// Expectations for shadow client (no locking logic)
	shadowMock.ExpectTxPipeline()
	shadowMock.ExpectDel("cache:negative:objects:elephant").SetVal(0)
	shadowMock.ExpectSet("cache:data:objects:elephant", `{"value":"value_for:elephant"}`, stale).SetVal("OK")
	shadowMock.ExpectSet("cache:fresh:objects:elephant", 1, fresh).SetVal("OK")
	shadowMock.ExpectTxPipelineExec()

	// Trigger Set
	err := cache.Set(ctx, "elephant", obj)

	assert.NoError(t, err)
	assert.NoError(t, cacheMock1.ExpectationsWereMet())
	assert.NoError(t, cacheMock2.ExpectationsWereMet())
	assertEventually(t, shadowMock.ExpectationsWereMet, "shadow mock expectations not met for multiple backend Set")
}

func TestCacheOperationsSucceedWhenShadowClientFails(t *testing.T) {
	ctx := context.Background()
	fresh := 10 * time.Second
	stale := 30 * time.Second
	negative := 5 * time.Second

	// Primary client (should succeed)
	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
		negative:   negative,
	}

	// Shadow client (configured to fail)
	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)
	expectedErr := errors.New("shadow client intentionally fails")

	// Setup failing shadow expectations - explicit calls
	// For cache.Set("elephant", ...)
	shadowMock.ExpectTxPipeline()
	shadowMock.Regexp().ExpectDel(".*").SetErr(expectedErr)
	shadowMock.Regexp().ExpectSet(".*", ".*", stale).SetErr(expectedErr)
	shadowMock.Regexp().ExpectSet(".*", 1, fresh).SetErr(expectedErr)
	shadowMock.ExpectTxPipelineExec().SetErr(expectedErr)

	// For cache.Get("elephant_get", ...) -> set
	shadowMock.ExpectTxPipeline()
	shadowMock.Regexp().ExpectDel(".*").SetErr(expectedErr)
	shadowMock.Regexp().ExpectSet(".*", ".*", stale).SetErr(expectedErr)
	shadowMock.Regexp().ExpectSet(".*", 1, fresh).SetErr(expectedErr)
	shadowMock.ExpectTxPipelineExec().SetErr(expectedErr)

	// For cache.Get("missing", ...) -> evict stops at the failed delete
	shadowMock.Regexp().ExpectDel(".*", ".*").SetErr(expectedErr)

	// Setup primary client expectations upfront
	cacheMock.ExpectCacheFill("elephant", testObj{Value: "shadow_failure_test"})
	cacheMock.ExpectCacheFetchEmpty("elephant_get")
	cacheMock.ExpectCacheFill("elephant_get", testObj{Value: "value_for:elephant_get"})
	cacheMock.ExpectCacheFetchEmpty("missing")
	cacheMock.ExpectCacheFillNegative("missing")

	// Create cache
	cache := NewCache[testObj](client, "objects", fresh, stale,
		WithNegativeCaching(negative),
		WithShadowWriteClient(shadowClient, 1*time.Second))

	// Test 1: Set succeeds despite shadow failure
	obj := testObj{Value: "shadow_failure_test"}
	err := cache.Set(ctx, "elephant", obj)
	assert.NoError(t, err, "cache.Set should succeed even if shadow write fails")

	// Test 2: Get succeeds despite shadow failure during fill
	v, err := cache.Get(ctx, "elephant_get", fetchTestObj)
	assert.NoError(t, err, "cache.Get should succeed even if shadow write fails during fill")
	if assert.NoError(t, err) {
		assert.Equal(t, "value_for:elephant_get", v.Value)
	}

	// Test 3: Negative caching succeeds despite shadow failure
	_, err = cache.Get(ctx, "missing", func(_ context.Context, _ string) (t testObj, err error) {
		return t, fmt.Errorf("not found: %w", ErrDoesNotExist)
	})
	assert.ErrorIs(t, err, ErrDoesNotExist, "cache.Get should return ErrDoesNotExist even if shadow negative write fails")

	// Verify primary mock completed
	assert.NoError(t, cacheMock.ExpectationsWereMet(), "Primary mock expectations should be met")

	// Verify shadow mock encountered errors asynchronously
	assertEventually(t, func() error {
		err := shadowMock.ExpectationsWereMet()
		// Expect *an* error here: either unmet expectations or expectations met with errors.
		if err == nil {
			return errors.New("shadowMock.ExpectationsWereMet() did not return an error as expected")
		}
		// Returning nil signals the condition (an error occurred) is met.
		return nil
	}, "shadow mock expectations not met or did not error as expected")
}

const (
	goneFresh    = 10 * time.Second
	goneStale    = 30 * time.Minute
	goneNegative = 10 * time.Second
)

type revocableSource struct {
	gone  atomic.Bool
	calls atomic.Int64
}

func (s *revocableSource) fetch(_ context.Context, _ string) (testObj, error) {
	s.calls.Add(1)
	if s.gone.Load() {
		return testObj{}, fmt.Errorf("revoked: %w", ErrDoesNotExist)
	}
	return testObj{Value: "alice"}, nil
}

func fastForward(d time.Duration, mrs ...*miniredis.Miniredis) {
	for _, mr := range mrs {
		mr.FastForward(d)
	}
}

func expireFreshAndAwaitRefresh(t *testing.T, c *Cache[testObj], key string, source *revocableSource, mrs ...*miniredis.Miniredis) {
	t.Helper()

	callsBefore := source.calls.Load()
	fastForward(goneFresh+time.Second, mrs...)
	_, _ = c.Get(context.Background(), key, source.fetch)

	lockKey := c.keysFor(key).lock
	assertEventually(t, func() error {
		for _, mr := range mrs {
			if mr.Exists(lockKey) {
				return errors.New("refresh still in flight")
			}
		}
		return nil
	}, "background refresh did not finish")
	require.EqualValues(t, 1, source.calls.Load()-callsBefore, "refresh did not consult the source")
}

func assertGoneAcrossNegativeExpiries(t *testing.T, c *Cache[testObj], key string, source *revocableSource, mrs ...*miniredis.Miniredis) {
	t.Helper()

	for i := range 3 {
		for range 2 {
			v, err := c.Get(context.Background(), key, source.fetch)
			require.Zero(t, v, "round %d: Get served the old value", i)
			require.ErrorIs(t, err, ErrDoesNotExist, "round %d", i)
		}
		fastForward(goneNegative+time.Second, mrs...)
	}
}

func TestCacheStopsServingValueOnceSourceReportsItGone(t *testing.T) {
	mr, rdb := test.MiniRedis(t)
	c := NewCache[testObj](rdb, "auth", goneFresh, goneStale, WithNegativeCaching(goneNegative))
	var source revocableSource

	require.NoError(t, c.Set(context.Background(), "tok", testObj{Value: "alice"}))
	source.gone.Store(true)

	expireFreshAndAwaitRefresh(t, c, "tok", &source, mr)
	assertGoneAcrossNegativeExpiries(t, c, "tok", &source, mr)
}

func TestCacheStopsServingValueOnceSourceReportsItGoneWithoutNegativeCaching(t *testing.T) {
	mr, rdb := test.MiniRedis(t)
	c := NewCache[testObj](rdb, "auth", goneFresh, goneStale)
	var source revocableSource

	require.NoError(t, c.Set(context.Background(), "tok", testObj{Value: "alice"}))
	source.gone.Store(true)

	expireFreshAndAwaitRefresh(t, c, "tok", &source, mr)
	assertGoneAcrossNegativeExpiries(t, c, "tok", &source, mr)
}

func TestCacheStopsServingValueFromAnyBackendOnceSourceReportsItGone(t *testing.T) {
	mr1, rdb1 := test.MiniRedis(t)
	mr2, rdb2 := test.MiniRedis(t)
	c := NewCacheMultipleBackends[testObj]([]redis.Cmdable{rdb1, rdb2}, "auth", goneFresh, goneStale, WithNegativeCaching(goneNegative))
	var source revocableSource

	require.NoError(t, c.Set(context.Background(), "tok", testObj{Value: "alice"}))
	require.True(t, mr1.Exists(c.keysFor("tok").data))
	require.True(t, mr2.Exists(c.keysFor("tok").data))
	source.gone.Store(true)

	expireFreshAndAwaitRefresh(t, c, "tok", &source, mr1, mr2)
	assertGoneAcrossNegativeExpiries(t, c, "tok", &source, mr1, mr2)
}

func TestCacheShadowClientDropsValueOnceSourceReportsItGone(t *testing.T) {
	mr, rdb := test.MiniRedis(t)
	shadowMr, shadowRdb := test.MiniRedis(t)
	c := NewCache[testObj](rdb, "auth", goneFresh, goneStale,
		WithNegativeCaching(goneNegative),
		WithShadowWriteClient(shadowRdb, time.Second))
	var source revocableSource
	keys := c.keysFor("tok")

	require.NoError(t, c.Set(context.Background(), "tok", testObj{Value: "alice"}))
	assertEventually(t, func() error {
		if !shadowMr.Exists(keys.data) {
			return errors.New("shadow write has not landed")
		}
		return nil
	}, "shadow client never received the value")
	source.gone.Store(true)

	expireFreshAndAwaitRefresh(t, c, "tok", &source, mr, shadowMr)

	assertEventually(t, func() error {
		if shadowMr.Exists(keys.data) {
			return errors.New("shadow client still holds the value")
		}
		return nil
	}, "shadow client kept the value after the source reported it gone")
}

// failDeletes rejects any DEL of cache data, sent alone or in a pipeline.
type failDeletes struct {
	armed atomic.Bool
	hits  atomic.Int32
}

func (*failDeletes) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failDeletes) rejects(cmd redis.Cmder) bool {
	return h.armed.Load() && cmd.Name() == "del" && strings.Contains(fmt.Sprint(cmd.Args()), "cache:data:")
}

func (h *failDeletes) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.rejects(cmd) {
			h.hits.Add(1)
			return errors.New("boom")
		}
		return next(ctx, cmd)
	}
}

func (h *failDeletes) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if h.rejects(cmd) {
				h.hits.Add(1)
				return errors.New("boom")
			}
		}
		return next(ctx, cmds)
	}
}

func TestCacheReturnsSourceErrorWhenRedisRejectsEviction(t *testing.T) {
	for name, opts := range map[string][]Option{
		"with negative caching":    {WithNegativeCaching(goneNegative)},
		"without negative caching": nil,
	} {
		t.Run(name, func(t *testing.T) {
			_, rdb := test.MiniRedis(t)
			hook := &failDeletes{}
			rdb.AddHook(hook)
			hook.armed.Store(true)
			c := NewCache[testObj](rdb, "auth", goneFresh, goneStale, opts...)
			var source revocableSource
			source.gone.Store(true)

			v, err := c.Get(context.Background(), "tok", source.fetch)

			assert.ErrorIs(t, err, ErrDoesNotExist)
			assert.NotContains(t, fmt.Sprint(err), "boom")
			assert.Zero(t, v)
			assert.Positive(t, hook.hits.Load(), "the delete never reached Redis")
		})
	}
}

// rejectNegativeMarker rejects SET on a negative key, as Redis does for writes
// at maxmemory. Redis aborts a whole MULTI when it rejects a queued command, so
// a transaction that holds the rejected SET runs none of its commands.
type rejectNegativeMarker struct{ hits atomic.Int32 }

func (*rejectNegativeMarker) DialHook(next redis.DialHook) redis.DialHook { return next }

func (*rejectNegativeMarker) rejects(cmd redis.Cmder) bool {
	args := cmd.Args()
	return cmd.Name() == "set" && len(args) > 1 && strings.HasPrefix(fmt.Sprint(args[1]), "cache:negative:")
}

func (h *rejectNegativeMarker) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.rejects(cmd) {
			h.hits.Add(1)
			err := errors.New("OOM command not allowed when used memory > 'maxmemory'")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func (h *rejectNegativeMarker) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if h.rejects(cmd) {
				h.hits.Add(1)
				return errors.New("EXECABORT Transaction discarded because of previous errors")
			}
		}
		return next(ctx, cmds)
	}
}

func TestCacheDropsValueWhenRedisRejectsNegativeMarker(t *testing.T) {
	mr, rdb := test.MiniRedis(t)
	hook := &rejectNegativeMarker{}
	rdb.AddHook(hook)
	c := NewCache[testObj](rdb, "auth", goneFresh, goneStale, WithNegativeCaching(goneNegative))
	require.NoError(t, c.Set(context.Background(), "tok", testObj{Value: "alice"}))
	var source revocableSource
	source.gone.Store(true)

	expireFreshAndAwaitRefresh(t, c, "tok", &source, mr)

	require.Positive(t, hook.hits.Load(), "the negative marker write never reached Redis")
	require.False(t, mr.Exists(c.keysFor("tok").data), "a rejected negative marker kept the value")
}
