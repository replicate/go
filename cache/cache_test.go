package cache

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
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

	name  string
	fresh time.Duration
	stale time.Duration
}

func (m mockWrapper) ExpectCacheFetchEmpty(key string) {
	m.ExpectMGet(
		"cache:fresh:"+m.name+":"+key,
		"cache:data:"+m.name+":"+key,
	).SetVal([]any{nil, nil})
}

func (m mockWrapper) ExpectCacheFetchErr(key string, err error) {
	m.ExpectMGet(
		"cache:fresh:"+m.name+":"+key,
		"cache:data:"+m.name+":"+key,
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
	).SetVal([]any{1, string(data)})
}

func (m mockWrapper) ExpectCacheFill(key string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	m.ExpectSet("cache:data:"+m.name+":"+key, string(data), m.stale).SetVal("OK")
	m.ExpectSet("cache:fresh:"+m.name+":"+key, 1, m.fresh).SetVal("OK")
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
}
