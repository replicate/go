// Package ratelimit implements a distributed rate limiter backed by a Redis
// server.
//
// The Limiter type provides a token bucket rate limiter with configurable
// throughput (rate) and capacity. The operation of the rate limiter is atomic
// and has should in principle be as accurate as the Redis server's system clock
// allows.
//
// Each token bucket is stored as a Redis hash in a single key, meaning that
// this package should work without modification in a Redis cluster environment,
// where you can control how limiters are distributed across slots using the
// usual mechanisms (e.g. "hash tags" in keys).
package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed limit.lua
	limitLuaCmd string
	//go:embed token_bucket.lua
	limiterCmd    string
	limiterScript = redis.NewScript("local " + strings.Join([]string{limitLuaCmd, limiterCmd}, "\n"))

	ErrInvalidData   = errors.New("limiter: received invalid data")
	ErrNegativeInput = errors.New("limiter: input values must be non-negative")
	ErrNilClient     = errors.New("limiter: redis client is nil")
)

type Limiter struct {
	client redis.Cmdable
}

type Result struct {
	OK        bool          // whether the request was entirely fulfilled
	Tokens    int           // number of tokens granted
	Remaining int           // number of tokens remaining
	Reset     time.Duration // time until bucket is full
}

func NewLimiter(client redis.Cmdable) (Limiter, error) {
	if client == nil {
		return Limiter{}, ErrNilClient
	}
	return Limiter{client: client}, nil
}

// Prepare stores the limiter script in the Redis script cache so that it can be
// more efficiently called with EVALSHA.
func (l Limiter) Prepare(ctx context.Context) error {
	fmt.Println(limiterScript)
	return limiterScript.Load(ctx, l.client).Err()
}

// Take requests a specified number of tokens from the token bucket stored in
// the named key, while also specifying the desired rate and capacity for the
// bucket. It returns the Result of the request, and the first error
// encountered, if any.
//
// Note: if >1 tokens are requested the Result may indicate partial fulfillment
// of the request by setting OK == false but Tokens > 0 on the Result.
func (l Limiter) Take(ctx context.Context, key string, tokens int, rate float64, capacity int) (*Result, error) {
	if tokens < 0 {
		return nil, fmt.Errorf("%w (tokens=%d)", ErrNegativeInput, tokens)
	}
	if rate < 0 {
		return nil, fmt.Errorf("%w (rate=%f)", ErrNegativeInput, rate)
	}
	if capacity < 0 {
		return nil, fmt.Errorf("%w (capacity=%d)", ErrNegativeInput, capacity)
	}
	cmd := limiterScript.Run(ctx, l.client, []string{key}, tokens, rate, capacity)
	return makeResult(tokens, cmd)
}

// SetOptions sets the desired rate and capacity for the token bucket stored in
// the named key. It returns the first error encountered, if any.
//
// Note that SetOptions applies a one minute TTL on the specified key, meaning
// that options will only be preserved if token requests against this key occur
// within that interval.
//
// SetOptions is provided so that a front-of-stack rate limiter can call Take
// without needing to know the (possibly user-dependent) rate and capacity for
// the specific limiter being queried. If the token is granted, the request can
// then look up the appropriate context for the request and call SetOptions to
// ensure that future requests are handled with the correct rate and capacity.
func (l Limiter) SetOptions(ctx context.Context, key string, rate float64, capacity int) error {
	if rate < 0 {
		return fmt.Errorf("%w (rate=%f)", ErrNegativeInput, rate)
	}
	if capacity < 0 {
		return fmt.Errorf("%w (capacity=%d)", ErrNegativeInput, capacity)
	}
	err := l.client.HSet(ctx, key, "rate", rate, "capacity", capacity).Err()
	if err != nil {
		return err
	}
	return l.client.Expire(ctx, key, time.Minute).Err()
}

func makeResult(tokens int, cmd *redis.Cmd) (*Result, error) {
	s, err := cmd.Int64Slice()
	if err != nil {
		return nil, err
	}
	if len(s) != 3 {
		return nil, fmt.Errorf("%w (len=%d)", ErrInvalidData, len(s))
	}
	result := &Result{
		OK:        int(s[0]) == tokens,
		Tokens:    int(s[0]),
		Remaining: int(s[1]),
		Reset:     time.Duration(s[2]) * time.Second,
	}
	return result, nil
}
