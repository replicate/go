// Package lock implements a basic distributed lock that is held in N redis
// instances.  Normally N should be 1, but the package supports multiple
// instances to support migrating from one instance to another.
//
// The locking primitives exposed by this package should not be used for any
// application in which the lock is critical for correctness. The guarantees
// that can be made by a lock of this nature are only suitable for applications
// which lock for efficiency (e.g. in order to avoid doing expensive work, such
// as refilling a cache, multiple times).
//
// When backed by multiple redis instances, locks will be acquired in the order
// given by the redis.Cmdable slice.  To avoid deadlocks, ensure every Locker
// client uses the same ordering of redis clients.
package lock

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/ksuid"
)

const retryInterval = 50 * time.Millisecond

var releaseScript = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)

var ErrLockNotAcquired = errors.New("locker: did not acquire lock")
var ErrLockNotHeld = errors.New("locker: lock was not held")

type Locker struct {
	Clients []redis.Cmdable

	tokenGenerator func() string // test seam
}

type Lock interface {
	Release(context.Context) error
}

type lock struct {
	clients []redis.Cmdable
	key     string
	token   string
}

// Prepare preloads any Lua scripts needed by locker. This allows later commands
// to use EVALSHA rather than straight EVAL. Calling Prepare is optional but
// recommended.
func (l Locker) Prepare(ctx context.Context) error {
	for _, client := range l.Clients {
		_, err := releaseScript.Load(ctx, client).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

// Acquire will attempt to acquire a lock at the specified key in Redis for the
// given duration. If it fails to acquire the lock because someone else is
// already holding it, it will retry until the passed context is canceled. If
// the context is canceled before the lock is acquired it will return the
// context error. It may also return other errors if it cannot communicate with
// Redis.
//
// Note: it is critical that the caller controls the blocking time by passing in
// a context that is cancelable or which has a deadline. If the context is never
// canceled and the lock cannot be acquired, the function will never return.
func (l Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (Lock, error) {
	for {
		lock, err := l.TryAcquire(ctx, key, ttl)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, ErrLockNotAcquired) {
			return nil, err
		}

		select {
		case <-time.After(retryInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TryAcquire attempts to acquire a lock at the specified key in Redis for the
// given duration. If it fails to acquire the lock because someone else is
// already holding it, it will return ErrLockNotAcquired. It may also return
// other errors if it cannot communicate with Redis.
func (l Locker) TryAcquire(ctx context.Context, key string, ttl time.Duration) (Lock, error) {
	if l.tokenGenerator == nil {
		l.tokenGenerator = generateKSUID
	}
	token := l.tokenGenerator()

	ret := lock{
		clients: l.Clients,
		key:     key,
		token:   token,
	}
	for i, client := range l.Clients {
		ok, err := client.SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			releaseErr := ret.release(ctx, i)
			return nil, errors.Join(err, releaseErr)
		}
		if !ok {
			releaseErr := ret.release(ctx, i)
			return nil, errors.Join(ErrLockNotAcquired, releaseErr)
		}
	}

	return &ret, nil
}

// Release attempts to release the lock in Redis. If the lock has already
// expired, or if the lock is held by another party, it will return
// ErrLockNotHeld. It may also return errors if it cannot communicate with
// Redis.
func (l *lock) Release(ctx context.Context) error {
	return l.release(ctx, len(l.clients))
}

func (l *lock) release(ctx context.Context, n int) error {
	errs := []error{}

	// We release locks in the opposite order from acquiring them, to prevent deadlocks
	for i := n - 1; i >= 0; i-- {
		result, err := releaseScript.Run(ctx, l.clients[i], []string{l.key}, l.token).Result()
		if err != nil {
			errs = append(errs, err)
		}

		if i, ok := result.(int64); !ok || i != 1 {
			errs = append(errs, ErrLockNotHeld)
		}
	}
	return errors.Join(errs...)
}

func generateKSUID() string {
	return ksuid.New().String()
}
