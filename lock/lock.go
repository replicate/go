// Package lock implements a basic distributed lock that is held in a single
// Redis instance.
//
// The locking primitives exposed by this package should not be used for any
// application in which the lock is critical for correctness. The guarantees
// that can be made by a lock of this nature are only suitable for applications
// which lock for efficiency (e.g. in order to avoid doing expensive work, such
// as refilling a cache, multiple times).
package lock

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/ksuid"
)

const retryInterval = 50 * time.Millisecond

var releaseScript = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)

var ErrLockNotAcquired = errors.New("locker: did not acquire lock")
var ErrLockNotHeld = errors.New("locker: lock was not held")

type Locker struct {
	Client redis.Cmdable

	tokenGenerator func() string // test seam
}

type Lock interface {
	Release(context.Context) error
}

type lock struct {
	client redis.Cmdable
	key    string
	token  string
}

// Prepare preloads any Lua scripts needed by locker. This allows later commands
// to use EVALSHA rather than straight EVAL. Calling Prepare is optional but
// recommended.
func (l Locker) Prepare(ctx context.Context) error {
	_, err := releaseScript.Load(ctx, l.Client).Result()
	if err != nil {
		return err
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
		if err != ErrLockNotAcquired {
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

	ok, err := l.Client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}

	ret := lock{
		client: l.Client,
		key:    key,
		token:  token,
	}
	return &ret, nil
}

// Release attempts to release the lock in Redis. If the lock has already
// expired, or if the lock is held by another party, it will return
// ErrLockNotHeld. It may also return errors if it cannot communicate with
// Redis.
func (l *lock) Release(ctx context.Context) error {
	result, err := releaseScript.Run(ctx, l.client, []string{l.key}, l.token).Result()
	if err != nil {
		return err
	}

	if i, ok := result.(int64); !ok || i != 1 {
		return ErrLockNotHeld
	}
	return nil
}

func generateKSUID() string {
	return ksuid.New().String()
}
