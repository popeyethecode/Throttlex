// Package limiter implements rate-limiting algorithms behind one small interface.
//
// The package has no HTTP code, so any Go program (an HTTP server, a gRPC
// service, a job worker) can use it. Every in-memory limiter is safe for
// concurrent use by many goroutines.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// Limiter decides whether a request identified by key may proceed.
//
// Allow both checks and consumes quota in one step. Splitting "check" and
// "consume" into two calls would let two concurrent requests both pass the
// check before either consumes, which is exactly the race we must avoid.
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}

// Decision is the result of one Allow call.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the maximum number of requests (or bucket capacity) for the key.
	Limit int
	// Remaining is how many more requests would be allowed right now.
	Remaining int
	// RetryAfter is how long to wait before a retry can succeed.
	// It is zero when the request was allowed.
	RetryAfter time.Duration
	// ResetAfter is how long until the key's quota is fully restored.
	ResetAfter time.Duration
}

// Errors returned by limiters. Callers can test for them with errors.Is.
var (
	// ErrInvalidConfig is wrapped by constructor errors for bad settings.
	ErrInvalidConfig = errors.New("invalid limiter config")
	// ErrTooManyKeys is returned when tracking a new key would exceed MaxKeys.
	ErrTooManyKeys = errors.New("too many tracked keys")
	// ErrEmptyKey is returned when Allow is called with an empty key.
	ErrEmptyKey = errors.New("empty key")
)

// Options holds settings shared by every in-memory algorithm.
type Options struct {
	// MaxKeys bounds how many distinct keys are tracked at once.
	// Zero means DefaultMaxKeys.
	MaxKeys int
	// Clock supplies the current time. Nil means the real clock.
	// Tests pass a ManualClock to control time precisely.
	Clock Clock
	// Shards is the number of lock shards (see store.go). Zero means 64.
	// Exposed mainly so benchmarks can compare locking strategies.
	Shards int
}

// DefaultMaxKeys is used when Options.MaxKeys is zero.
const DefaultMaxKeys = 100_000

// normalize validates o and fills in defaults for zero values.
func (o Options) normalize() (Options, error) {
	if o.MaxKeys < 0 {
		return o, fmt.Errorf("%w: MaxKeys must not be negative, got %d", ErrInvalidConfig, o.MaxKeys)
	}
	if o.Shards < 0 {
		return o, fmt.Errorf("%w: Shards must not be negative, got %d", ErrInvalidConfig, o.Shards)
	}
	if o.MaxKeys == 0 {
		o.MaxKeys = DefaultMaxKeys
	}
	if o.Shards == 0 {
		o.Shards = numShards
	}
	if o.Clock == nil {
		o.Clock = RealClock{}
	}
	return o, nil
}

// validateLimitWindow checks the settings shared by the window algorithms.
func validateLimitWindow(limit int, window time.Duration) error {
	if limit < 1 {
		return fmt.Errorf("%w: limit must be at least 1, got %d", ErrInvalidConfig, limit)
	}
	if window <= 0 {
		return fmt.Errorf("%w: window must be positive, got %s", ErrInvalidConfig, window)
	}
	return nil
}

// epsilon absorbs floating-point rounding error in algorithms that use
// fractional counts, so that waiting exactly the advertised RetryAfter
// really does succeed. It is far smaller than one request.
const epsilon = 1e-9

// windowStart returns the start of the fixed window containing nowNs
// (Unix nanoseconds), with windows aligned to the Unix epoch.
func windowStart(nowNs int64, window time.Duration) int64 {
	w := int64(window)
	start := nowNs - nowNs%w
	if nowNs < 0 && nowNs%w != 0 { // % rounds toward zero; floor instead for pre-1970 times
		start -= w
	}
	return start
}

// ceilNanos converts a fractional number of nanoseconds into a duration,
// rounding up so a caller who waits that long is not told to wait again.
func ceilNanos(ns float64) time.Duration {
	if ns <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(ns))
}
