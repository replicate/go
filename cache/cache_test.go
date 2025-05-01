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

	client, mock := redismock.NewClientMock()
	cacheMock := mockWrapper{
		ClientMock: mock,
		name:       "objects",
		fresh:      fresh,
		stale:      stale,
	}

	shadowClient, shadowMock := redismock.NewClientMock()
	shadowMock.MatchExpectationsInOrder(false)

	cache := NewCache[testObj](client, "objects", fresh, stale, WithShadowWriteClient(shadowClient))

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

	cache := NewCache[testObj](client, "objects", fresh, stale, WithShadowWriteClient(shadowClient))

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
		WithShadowWriteClient(shadowClient))

	// Expectations for primary client
	cacheMock.ExpectCacheFetchEmpty("elephant")
	cacheMock.ExpectCacheFillNegative("elephant")

	// Expectations for shadow client (negative write only)
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
		WithShadowWriteClient(shadowClient),
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

	// For cache.Get("missing", ...) -> setNegative
	shadowMock.Regexp().ExpectSet(".*", 1, negative).SetErr(expectedErr)

	// Setup primary client expectations upfront
	cacheMock.ExpectCacheFill("elephant", testObj{Value: "shadow_failure_test"})
	cacheMock.ExpectCacheFetchEmpty("elephant_get")
	cacheMock.ExpectCacheFill("elephant_get", testObj{Value: "value_for:elephant_get"})
	cacheMock.ExpectCacheFetchEmpty("missing")
	cacheMock.ExpectCacheFillNegative("missing")

	// Create cache
	cache := NewCache[testObj](client, "objects", fresh, stale,
		WithNegativeCaching(negative),
		WithShadowWriteClient(shadowClient))

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
