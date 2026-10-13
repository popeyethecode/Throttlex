package limiter

import (
	"context"
	"fmt"
	"math"
	"time"
)

// TokenBucket gives each key a bucket holding up to capacity tokens.
// Every allowed request takes one token, and tokens flow back in at
// refillRate tokens per second.
//
// Unlike the window algorithms it allows short bursts (up to capacity)
// while enforcing a smooth long-run average rate (refillRate). There is no
// boundary burst, because refill is continuous rather than reset at fixed
// moments.
//
// The bucket is not refilled by a background timer. Instead, each request
// computes how many tokens arrived since the last request from the elapsed
// time. That makes an idle key cost nothing.
type TokenBucket struct {
	*store[tokenBucketState]
	capacity int
	rate     float64 // tokens per second
}

type tokenBucketState struct {
	tokens  float64
	last    time.Time // when tokens was last brought up to date
	started bool
}

// NewTokenBucket returns a TokenBucket limiter.
func NewTokenBucket(capacity int, refillPerSecond float64, opts Options) (*TokenBucket, error) {
	if capacity < 1 {
		return nil, fmt.Errorf("%w: capacity must be at least 1, got %d", ErrInvalidConfig, capacity)
	}
	if !(refillPerSecond > 0) || math.IsInf(refillPerSecond, 0) {
		return nil, fmt.Errorf("%w: refill rate must be a positive number, got %v", ErrInvalidConfig, refillPerSecond)
	}
	opts, err := opts.normalize()
	if err != nil {
		return nil, err
	}
	return &TokenBucket{
		store:    newStore[tokenBucketState](opts.Shards, opts.MaxKeys, opts.Clock),
		capacity: capacity,
		rate:     refillPerSecond,
	}, nil
}

// Allow implements Limiter.
func (l *TokenBucket) Allow(_ context.Context, key string) (Decision, error) {
	return l.update(key, func(st *tokenBucketState, now time.Time) (Decision, time.Time) {
		capacity := float64(l.capacity)
		if !st.started {
			// A new key starts with a full bucket.
			st.tokens, st.last, st.started = capacity, now, true
		} else if elapsed := now.Sub(st.last); elapsed > 0 {
			st.tokens = math.Min(capacity, st.tokens+elapsed.Seconds()*l.rate)
			st.last = now
		}

		d := Decision{Limit: l.capacity}
		if st.tokens >= 1-epsilon {
			st.tokens = math.Max(0, st.tokens-1)
			d.Allowed = true
		} else {
			d.RetryAfter = l.timeToRefill(1 - st.tokens)
		}
		d.Remaining = int(math.Floor(st.tokens + epsilon))

		// Once the bucket is full again, the state is identical to a new key.
		d.ResetAfter = l.timeToRefill(capacity - st.tokens)
		return d, now.Add(d.ResetAfter)
	})
}

// timeToRefill returns how long it takes to gain n tokens.
func (l *TokenBucket) timeToRefill(n float64) time.Duration {
	return ceilNanos(n / l.rate * float64(time.Second))
}
