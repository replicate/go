package lock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
)

// TODO: these test don't really test the locking approach -- they just test
// that we've implemented the various communications with Redis correctly. In
// future it would be nice to have at least one functional test which ensures
// that locking behaves as expected under concurrent access.

func TestLockerTryAcquireReturnsLockWhenSetSucceeds(t *testing.T) {
	ctx := context.Background()
	k := "somekey"
	client, mock := redismock.NewClientMock()
	locker := Locker{
		Client:         client,
		tokenGenerator: func() string { return "giraffe" },
	}

	mock.ExpectSetNX(k, "giraffe", 1*time.Second).SetVal(true)

	l, err := locker.TryAcquire(ctx, k, 1*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, l)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLockerTryAcquireReturnsErrLockNotAcquiredWhenSetFails(t *testing.T) {
	ctx := context.Background()
	k := "somekey"
	client, mock := redismock.NewClientMock()
	locker := Locker{
		Client:         client,
		tokenGenerator: func() string { return "elephant" },
	}

	mock.ExpectSetNX(k, "elephant", 1*time.Second).SetVal(false)

	l, err := locker.TryAcquire(ctx, k, 1*time.Second)

	assert.ErrorIs(t, err, ErrLockNotAcquired)
	assert.Nil(t, l)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLockerTryAcquireReturnsRedisErrors(t *testing.T) {
	ctx := context.Background()
	k := "somekey"
	client, mock := redismock.NewClientMock()
	locker := Locker{
		Client:         client,
		tokenGenerator: func() string { return "moose" },
	}

	mock.ExpectSetNX(k, "moose", 1*time.Second).SetErr(errors.New("kaboom"))

	l, err := locker.TryAcquire(ctx, k, 1*time.Second)

	assert.ErrorContains(t, err, "kaboom")
	assert.Nil(t, l)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLockReleaseReturnsNilWhenLockSuccessfullyReleased(t *testing.T) {
	ctx := context.Background()
	k := "somekey"
	client, mock := redismock.NewClientMock()
	locker := Locker{
		Client:         client,
		tokenGenerator: func() string { return "platypus" },
	}

	mock.Regexp().ExpectScriptLoad(`if redis.call\("get", KEYS\[1\]\) .+`).SetVal(releaseScript.Hash())
	mock.ExpectSetNX(k, "platypus", 1*time.Second).SetVal(true)
	mock.ExpectEvalSha(releaseScript.Hash(), []string{k}, "platypus").SetVal(int64(1))

	err := locker.Prepare(ctx)
	assert.NoError(t, err)

	l, err := locker.TryAcquire(ctx, k, 1*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, l)

	err = l.Release(ctx)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLockReleaseReturnsErrLockNotHeldIfLockWasNotReleased(t *testing.T) {
	ctx := context.Background()
	k := "somekey"
	client, mock := redismock.NewClientMock()
	locker := Locker{
		Client:         client,
		tokenGenerator: func() string { return "platypus" },
	}

	mock.Regexp().ExpectScriptLoad(`if redis.call\("get", KEYS\[1\]\) .+`).SetVal(releaseScript.Hash())
	mock.ExpectSetNX(k, "platypus", 1*time.Second).SetVal(true)
	mock.ExpectEvalSha(releaseScript.Hash(), []string{k}, "platypus").SetVal(int64(0))

	err := locker.Prepare(ctx)
	assert.NoError(t, err)

	l, err := locker.TryAcquire(ctx, k, 1*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, l)

	err = l.Release(ctx)

	assert.ErrorIs(t, err, ErrLockNotHeld)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLockReleaseReturnsRedisErrors(t *testing.T) {
	ctx := context.Background()
	k := "somekey"
	client, mock := redismock.NewClientMock()
	locker := Locker{
		Client:         client,
		tokenGenerator: func() string { return "platypus" },
	}

	mock.Regexp().ExpectScriptLoad(`if redis.call\("get", KEYS\[1\]\) .+`).SetVal(releaseScript.Hash())
	mock.ExpectSetNX(k, "platypus", 1*time.Second).SetVal(true)
	mock.ExpectEvalSha(releaseScript.Hash(), []string{k}, "platypus").SetErr(errors.New("boom"))

	err := locker.Prepare(ctx)
	assert.NoError(t, err)

	l, err := locker.TryAcquire(ctx, k, 1*time.Second)

	assert.NoError(t, err)
	assert.NotNil(t, l)

	err = l.Release(ctx)

	assert.ErrorContains(t, err, "boom")
	assert.NoError(t, mock.ExpectationsWereMet())
}
