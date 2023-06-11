package lock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestLockAcquireIntegration(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not set")
	}

	ctx := context.Background()

	opts, err := redis.ParseURL(redisURL)
	require.NoError(t, err)

	client := redis.NewClient(opts)
	locker := Locker{Client: client}

	require.NoError(t, locker.Prepare(ctx))

	start := make(chan struct{})
	results := make(chan string, 50)
	var wg sync.WaitGroup

	// Start 50 goroutines which all attempt to acquire the lock at the same
	// moment, synchronized by a channel closure.
	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			<-start

			// Each goroutine will wait for up to 1s for the lock...
			timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			lock, err := locker.Acquire(timeoutCtx, "monkey", 2*time.Second)
			if errors.Is(err, context.DeadlineExceeded) {
				return
			}
			require.NoError(t, err)

			// ...and will hold onto it for approximately 100 milliseconds.
			results <- fmt.Sprintf("lock acquired by goroutine %d", id)
			time.Sleep(100 * time.Millisecond)

			require.NoError(t, lock.Release(ctx))
		}(i)
	}

	// Release the goroutines!
	close(start)
	wg.Wait()

	// With each lock held for ~100ms, somewhere between 9 and 11 goroutines
	// should have got the lock.
	l := len(results)
	require.GreaterOrEqual(t, l, 9)
	require.LessOrEqual(t, l, 11)
}

func TestLockTryAcquireIntegration(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not set")
	}

	ctx := context.Background()

	opts, err := redis.ParseURL(redisURL)
	require.NoError(t, err)

	client := redis.NewClient(opts)
	locker := Locker{Client: client}

	require.NoError(t, locker.Prepare(ctx))

	start := make(chan struct{})
	results := make(chan string, 50)
	var wg sync.WaitGroup

	// Start 50 goroutines which all attempt to acquire the lock at the same
	// moment, synchronized by a channel closure.
	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			<-start

			lock, err := locker.TryAcquire(ctx, "giraffe", 1*time.Second)
			if err == ErrLockNotAcquired {
				return
			}
			require.NoError(t, err)

			results <- fmt.Sprintf("lock acquired by goroutine %d", id)
			time.Sleep(100 * time.Millisecond)

			require.NoError(t, lock.Release(ctx))
		}(i)
	}

	// Release the goroutines!
	close(start)
	wg.Wait()

	// Check that only one goroutine got the lock
	require.Equal(t, 1, len(results))
}
