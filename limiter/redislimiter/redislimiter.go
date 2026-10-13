// Package redislimiter implements limiters whose state lives in Redis, so
// every instance of a service shares one quota per client.
//
// A sync.Mutex only protects memory inside one process; it does nothing
// for two processes talking to the same Redis. Atomicity here comes from
// Redis itself: each Allow runs a Lua script, and Redis executes a script
// from start to finish without running any other command in between. So
// "read the state, decide, write the new state" cannot interleave with
// another instance's request.
//
// Only FixedWindow and TokenBucket are implemented. Each is a handful of
// O(1) operations on one small key, which is what makes them a good fit
// for Redis. A sliding log would need a sorted set holding one entry per
// request, i.e. far more memory and work per call.
package redislimiter

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/popeyethecode/throttlex/limiter"
)

// Options configures the Redis limiters.
type Options struct {
	// Prefix is prepended to every Redis key. Default "throttlex:".
	Prefix string
	// Timeout bounds each Redis round trip. Default 100ms. A rate limiter
	// that hangs is worse than one that fails fast and applies the
	// configured failure policy.
	Timeout time.Duration
}

func (o Options) normalize() Options {
	if o.Prefix == "" {
		o.Prefix = "throttlex:"
	}
	if o.Timeout <= 0 {
		o.Timeout = 100 * time.Millisecond
	}
	return o
}

// fixedWindowScript counts requests in a window that starts at a key's
// first request and lasts window milliseconds (enforced by the key's TTL).
// Denied requests are not counted, so the counter never exceeds limit.
//
// KEYS[1] = counter key, ARGV[1] = window in ms, ARGV[2] = limit.
// Returns {allowed (0/1), count, ms until the window resets}.
var fixedWindowScript = redis.NewScript(`
local limit = tonumber(ARGV[2])
local count = tonumber(redis.call('GET', KEYS[1]) or '0')
local allowed = 0
if count < limit then
  count = redis.call('INCR', KEYS[1])
  allowed = 1
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  ttl = tonumber(ARGV[1])
end
return {allowed, count, ttl}
`)

// tokenBucketScript refills and takes a token in one atomic step. It reads
// the time from Redis (TIME) rather than trusting each app server's clock,
// so instances with skewed clocks still agree on how many tokens exist.
//
// KEYS[1] = bucket hash, ARGV[1] = capacity, ARGV[2] = tokens per second.
// Returns {allowed (0/1), whole tokens left, retry ms, ms until full}.
var tokenBucketScript = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2]) / 1000
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) * rate)
  ts = now
end

local allowed = 0
local retry = 0
if tokens >= 1 - 1e-9 then
  tokens = math.max(0, tokens - 1)
  allowed = 1
else
  retry = math.ceil((1 - tokens) / rate)
end
local reset = math.ceil((capacity - tokens) / rate)

redis.call('HSET', KEYS[1], 'tokens', string.format('%.17g', tokens), 'ts', string.format('%.0f', ts))
redis.call('PEXPIRE', KEYS[1], reset + 1000)
return {allowed, math.floor(tokens + 1e-9), retry, reset}
`)

// FixedWindow is a Redis-backed fixed-window limiter.
//
// Unlike limiter.FixedWindow, whose windows align to the clock (12:00,
// 12:01, ...), each key's window here starts at its first request. That
// avoids depending on app-server clocks and spreads expirations out; the
// boundary-burst caveat still applies.
type FixedWindow struct {
	client redis.Scripter
	limit  int
	window time.Duration
	opts   Options
}

// NewFixedWindow returns a Redis-backed FixedWindow limiter.
func NewFixedWindow(client redis.Scripter, limit int, window time.Duration, opts Options) (*FixedWindow, error) {
	if limit < 1 {
		return nil, fmt.Errorf("%w: limit must be at least 1, got %d", limiter.ErrInvalidConfig, limit)
	}
	if window < time.Millisecond {
		return nil, fmt.Errorf("%w: window must be at least 1ms, got %s", limiter.ErrInvalidConfig, window)
	}
	return &FixedWindow{client: client, limit: limit, window: window, opts: opts.normalize()}, nil
}

// Allow implements limiter.Limiter.
func (l *FixedWindow) Allow(ctx context.Context, key string) (limiter.Decision, error) {
	if key == "" {
		return limiter.Decision{}, limiter.ErrEmptyKey
	}
	ctx, cancel := context.WithTimeout(ctx, l.opts.Timeout)
	defer cancel()

	res, err := fixedWindowScript.Run(ctx, l.client, []string{l.opts.Prefix + "fw:" + key},
		l.window.Milliseconds(), l.limit).Int64Slice()
	if err != nil {
		return limiter.Decision{}, fmt.Errorf("redis fixed window: %w", err)
	}
	if len(res) != 3 {
		return limiter.Decision{}, fmt.Errorf("redis fixed window: unexpected reply %v", res)
	}

	reset := time.Duration(res[2]) * time.Millisecond
	d := limiter.Decision{
		Allowed:    res[0] == 1,
		Limit:      l.limit,
		Remaining:  max(0, l.limit-int(res[1])),
		ResetAfter: reset,
	}
	if !d.Allowed {
		d.RetryAfter = reset
	}
	return d, nil
}

// TokenBucket is a Redis-backed token-bucket limiter.
type TokenBucket struct {
	client   redis.Scripter
	capacity int
	rate     float64
	opts     Options
}

// NewTokenBucket returns a Redis-backed TokenBucket limiter.
func NewTokenBucket(client redis.Scripter, capacity int, refillPerSecond float64, opts Options) (*TokenBucket, error) {
	if capacity < 1 {
		return nil, fmt.Errorf("%w: capacity must be at least 1, got %d", limiter.ErrInvalidConfig, capacity)
	}
	if !(refillPerSecond > 0) || math.IsInf(refillPerSecond, 0) {
		return nil, fmt.Errorf("%w: refill rate must be a positive number, got %v", limiter.ErrInvalidConfig, refillPerSecond)
	}
	return &TokenBucket{client: client, capacity: capacity, rate: refillPerSecond, opts: opts.normalize()}, nil
}

// Allow implements limiter.Limiter.
func (l *TokenBucket) Allow(ctx context.Context, key string) (limiter.Decision, error) {
	if key == "" {
		return limiter.Decision{}, limiter.ErrEmptyKey
	}
	ctx, cancel := context.WithTimeout(ctx, l.opts.Timeout)
	defer cancel()

	res, err := tokenBucketScript.Run(ctx, l.client, []string{l.opts.Prefix + "tb:" + key},
		l.capacity, l.rate).Int64Slice()
	if err != nil {
		return limiter.Decision{}, fmt.Errorf("redis token bucket: %w", err)
	}
	if len(res) != 4 {
		return limiter.Decision{}, fmt.Errorf("redis token bucket: unexpected reply %v", res)
	}
	return limiter.Decision{
		Allowed:    res[0] == 1,
		Limit:      l.capacity,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
		ResetAfter: time.Duration(res[3]) * time.Millisecond,
	}, nil
}
