package redislimiter

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/popeyethecode/throttlex/limiter"
)

// testRedis returns a Redis client for tests and a unique key prefix.
//
// If THROTTLEX_TEST_REDIS_ADDR is set (e.g. by `docker compose` or CI), the
// tests run against that real Redis. Otherwise they use miniredis, an
// in-process Redis implementation, so `go test ./...` works anywhere. The
// returned *miniredis.Miniredis is nil for real Redis; tests that need to
// move time forward skip in that case instead of sleeping.
func testRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis, Options) {
	t.Helper()
	opts := Options{Prefix: "test:" + t.Name() + ":" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":", Timeout: time.Second}

	if addr := os.Getenv("THROTTLEX_TEST_REDIS_ADDR"); addr != "" {
		client := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { client.Close() })
		if err := client.Ping(context.Background()).Err(); err != nil {
			t.Fatalf("THROTTLEX_TEST_REDIS_ADDR=%s is set but Redis is unreachable: %v", addr, err)
		}
		return client, nil, opts
	}

	mr := miniredis.RunT(t)
	mr.SetTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return client, mr, opts
}

func TestFixedWindow(t *testing.T) {
	client, mr, opts := testRedis(t)
	l, err := NewFixedWindow(client, 3, time.Minute, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i, wantRemaining := range []int{2, 1, 0} {
		d, err := l.Allow(ctx, "client")
		if err != nil || !d.Allowed || d.Remaining != wantRemaining {
			t.Fatalf("request %d: %+v, %v", i+1, d, err)
		}
	}
	d, err := l.Allow(ctx, "client")
	if err != nil || d.Allowed || d.Remaining != 0 {
		t.Fatalf("4th request should be denied: %+v, %v", d, err)
	}
	if d.RetryAfter <= 59*time.Second || d.RetryAfter > time.Minute {
		t.Fatalf("RetryAfter = %s, want just under 1m", d.RetryAfter)
	}
	if d, _ := l.Allow(ctx, "other"); !d.Allowed {
		t.Fatal("a different client should have its own quota")
	}

	if mr == nil {
		t.Skip("window expiry needs miniredis to fast-forward time")
	}
	mr.FastForward(time.Minute)
	if d, _ := l.Allow(ctx, "client"); !d.Allowed || d.Remaining != 2 {
		t.Fatalf("after the window expires the quota should reset: %+v", d)
	}
}

func TestTokenBucket(t *testing.T) {
	client, mr, opts := testRedis(t)
	l, err := NewTokenBucket(client, 3, 1, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i, wantRemaining := range []int{2, 1, 0} {
		d, err := l.Allow(ctx, "client")
		if err != nil || !d.Allowed || d.Remaining != wantRemaining {
			t.Fatalf("request %d: %+v, %v", i+1, d, err)
		}
	}
	d, err := l.Allow(ctx, "client")
	if err != nil || d.Allowed {
		t.Fatalf("4th request should be denied: %+v, %v", d, err)
	}
	// Real Redis time moves a few ms between calls, so allow some slack.
	if d.RetryAfter <= 900*time.Millisecond || d.RetryAfter > time.Second {
		t.Fatalf("RetryAfter = %s, want about 1s", d.RetryAfter)
	}

	if mr == nil {
		t.Skip("refill needs miniredis to move time")
	}
	mr.SetTime(time.Date(2026, 1, 1, 12, 0, 1, 0, time.UTC)) // +1s: one token back
	if d, _ := l.Allow(ctx, "client"); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("after 1s one token should have refilled: %+v", d)
	}
	mr.SetTime(time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC)) // +1h: never above capacity
	if d, _ := l.Allow(ctx, "client"); !d.Allowed || d.Remaining != 2 {
		t.Fatalf("bucket should be capped at capacity: %+v", d)
	}
}

// TestSharedAcrossInstances simulates several app instances (separate
// clients and connection pools) hammering one key concurrently. The Lua
// scripts must make the total accepted exactly the limit.
func TestSharedAcrossInstances(t *testing.T) {
	const limit, instances, perInstance = 50, 4, 100

	for _, algo := range []string{"fixed_window", "token_bucket"} {
		t.Run(algo, func(t *testing.T) {
			first, mr, opts := testRedis(t)
			addr := first.Options().Addr
			if mr != nil {
				// Freeze miniredis time so the token bucket cannot refill mid-test.
				mr.SetTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
			}

			var allowed atomic.Int64
			var wg sync.WaitGroup
			for range instances {
				client := redis.NewClient(&redis.Options{Addr: addr})
				defer client.Close()

				var l limiter.Limiter
				var err error
				if algo == "fixed_window" {
					l, err = NewFixedWindow(client, limit, time.Hour, opts)
				} else {
					// ~0 refill, so real-Redis time passing cannot add tokens.
					l, err = NewTokenBucket(client, limit, 0.0001, opts)
				}
				if err != nil {
					t.Fatal(err)
				}

				for range 10 { // 10 goroutines per instance
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range perInstance / 10 {
							d, err := l.Allow(context.Background(), "shared-client")
							if err != nil {
								t.Error(err)
								return
							}
							if d.Allowed {
								allowed.Add(1)
							}
						}
					}()
				}
			}
			wg.Wait()

			if got := allowed.Load(); got != limit {
				t.Fatalf("allowed %d across %d instances, want exactly %d", got, instances, limit)
			}
		})
	}
}

func TestKeysExpire(t *testing.T) {
	client, _, opts := testRedis(t)
	ctx := context.Background()
	fw, _ := NewFixedWindow(client, 5, time.Minute, opts)
	tb, _ := NewTokenBucket(client, 5, 1, opts)
	fw.Allow(ctx, "k")
	tb.Allow(ctx, "k")

	for _, key := range []string{opts.Prefix + "fw:k", opts.Prefix + "tb:k"} {
		ttl, err := client.PTTL(ctx, key).Result()
		if err != nil || ttl <= 0 {
			t.Errorf("key %s has TTL %s (err %v); every key must expire", key, ttl, err)
		}
	}
}

func TestRedisUnavailable(t *testing.T) {
	// Nothing listens on port 1, so every call fails fast.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer client.Close()
	fw, _ := NewFixedWindow(client, 5, time.Minute, Options{Timeout: 500 * time.Millisecond})
	tb, _ := NewTokenBucket(client, 5, 1, Options{Timeout: 500 * time.Millisecond})

	for name, l := range map[string]limiter.Limiter{"fixed_window": fw, "token_bucket": tb} {
		start := time.Now()
		_, err := l.Allow(context.Background(), "k")
		if err == nil {
			t.Errorf("%s: expected an error with Redis down", name)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("%s: took %s to fail; the timeout should bound it", name, elapsed)
		}
	}
}

func TestRedisGoesAwayMidway(t *testing.T) {
	if os.Getenv("THROTTLEX_TEST_REDIS_ADDR") != "" {
		t.Skip("needs miniredis so the test can stop the server")
	}
	client, mr, opts := testRedis(t)
	tb, _ := NewTokenBucket(client, 5, 1, opts)
	if _, err := tb.Allow(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	mr.Close()
	if _, err := tb.Allow(context.Background(), "k"); err == nil {
		t.Fatal("expected an error after Redis stopped")
	}
}

func TestCancelledContext(t *testing.T) {
	client, _, opts := testRedis(t)
	tb, _ := NewTokenBucket(client, 5, 1, opts)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tb.Allow(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestInvalidConfig(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	errs := []error{}
	_, err := NewFixedWindow(client, 0, time.Minute, Options{})
	errs = append(errs, err)
	_, err = NewFixedWindow(client, 1, 0, Options{})
	errs = append(errs, err)
	_, err = NewTokenBucket(client, 0, 1, Options{})
	errs = append(errs, err)
	_, err = NewTokenBucket(client, 1, -1, Options{})
	errs = append(errs, err)
	for i, err := range errs {
		if !errors.Is(err, limiter.ErrInvalidConfig) {
			t.Errorf("case %d: got %v, want ErrInvalidConfig", i, err)
		}
	}
}
