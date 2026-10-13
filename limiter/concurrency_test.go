package limiter_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/popeyethecode/throttlex/limiter"
)

// The clock is frozen in these stress tests, so no window moves and no
// tokens refill. Every algorithm must then accept exactly `limit` requests
// per key, no matter how the goroutines interleave. Run them with -race.

func TestConcurrentSameKeyNeverExceedsLimit(t *testing.T) {
	const (
		limit      = 100
		goroutines = 50
		perG       = 20 // 1000 attempts in total, 10x the limit
	)
	for _, name := range []string{"fixed_window", "sliding_log", "sliding_counter", "token_bucket"} {
		t.Run(name, func(t *testing.T) {
			clk := limiter.NewManualClock(t0)
			l := algorithms(t, limit, limiter.Options{Clock: clk})[name]

			var allowed atomic.Int64
			var wg sync.WaitGroup
			for range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range perG {
						d, err := l.Allow(context.Background(), "hot-key")
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
			wg.Wait()

			if got := allowed.Load(); got != limit {
				t.Fatalf("allowed %d requests, want exactly %d", got, limit)
			}
		})
	}
}

func TestConcurrentDifferentKeysAreIndependent(t *testing.T) {
	const (
		limit = 10
		keys  = 200
		perK  = 25
	)
	for _, name := range []string{"fixed_window", "sliding_log", "sliding_counter", "token_bucket"} {
		t.Run(name, func(t *testing.T) {
			clk := limiter.NewManualClock(t0)
			l := algorithms(t, limit, limiter.Options{Clock: clk})[name]
			sweeper := l.(limiter.Sweeper)

			// Run cleanup in a tight loop at the same time, to prove that
			// sweeping cannot corrupt or reset live state.
			stop := make(chan struct{})
			sweepDone := make(chan struct{})
			go func() {
				defer close(sweepDone)
				for {
					select {
					case <-stop:
						return
					default:
						sweeper.Sweep()
					}
				}
			}()

			counts := make([]atomic.Int64, keys)
			var wg sync.WaitGroup
			for k := range keys {
				// Two goroutines per key, so keys also see same-key contention.
				for range 2 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						key := "client-" + strconv.Itoa(k)
						for range perK {
							if d, _ := l.Allow(context.Background(), key); d.Allowed {
								counts[k].Add(1)
							}
						}
					}()
				}
			}
			wg.Wait()
			close(stop)
			<-sweepDone

			for k := range counts {
				if got := counts[k].Load(); got != limit {
					t.Fatalf("key %d: allowed %d, want %d", k, got, limit)
				}
			}
			if got := sweeper.Len(); got != keys {
				t.Fatalf("tracking %d keys, want %d", got, keys)
			}
		})
	}
}

func TestSweepRemovesOnlyExpiredState(t *testing.T) {
	clk := limiter.NewManualClock(t0)
	tb, _ := limiter.NewTokenBucket(2, 1, limiter.Options{Clock: clk}) // refills 1 token/s
	ctx := context.Background()

	tb.Allow(ctx, "a") // a: 1 token left, full again in 1s
	tb.Allow(ctx, "b")
	tb.Allow(ctx, "b") // b: 0 tokens left, full again in 2s

	if n := tb.Len(); n != 2 {
		t.Fatalf("Len = %d, want 2", n)
	}
	clk.Advance(999 * time.Millisecond)
	if n := tb.Sweep(); n != 0 {
		t.Fatalf("swept %d keys before any expired", n)
	}
	clk.Advance(time.Millisecond)
	if n := tb.Sweep(); n != 1 {
		t.Fatalf("swept %d keys at 1s, want 1 (only a)", n)
	}
	clk.Advance(time.Second)
	if n := tb.Sweep(); n != 1 {
		t.Fatalf("swept %d keys at 2s, want 1 (b)", n)
	}
	if n := tb.Len(); n != 0 {
		t.Fatalf("Len = %d after sweeping everything, want 0", n)
	}
}

func TestWindowStateExpiresAfterWindow(t *testing.T) {
	clk := limiter.NewManualClock(t0)
	opts := limiter.Options{Clock: clk}
	fw, _ := limiter.NewFixedWindow(5, time.Minute, opts)
	sl, _ := limiter.NewSlidingLog(5, time.Minute, opts)
	sc, _ := limiter.NewSlidingCounter(5, time.Minute, opts)

	for _, l := range []limiter.Limiter{fw, sl, sc} {
		l.Allow(context.Background(), "k")
	}
	clk.Advance(time.Minute)
	if n := fw.Sweep(); n != 1 {
		t.Errorf("fixed window: swept %d after one window, want 1", n)
	}
	if n := sl.Sweep(); n != 1 {
		t.Errorf("sliding log: swept %d after one window, want 1", n)
	}
	// The sliding counter's count still affects the next window's estimate.
	if n := sc.Sweep(); n != 0 {
		t.Errorf("sliding counter: swept %d after one window, want 0", n)
	}
	clk.Advance(time.Minute)
	if n := sc.Sweep(); n != 1 {
		t.Errorf("sliding counter: swept %d after two windows, want 1", n)
	}
}

func TestMaxKeysBoundsMemory(t *testing.T) {
	clk := limiter.NewManualClock(t0)
	// One shard, so the reclaim-on-insert below sees every key. With many
	// shards, only the new key's own shard is swept on insert (cheap), and
	// the background janitor reclaims the rest.
	l, _ := limiter.NewFixedWindow(1, time.Minute, limiter.Options{Clock: clk, MaxKeys: 2, Shards: 1})
	ctx := context.Background()

	for _, key := range []string{"a", "b"} {
		if _, err := l.Allow(ctx, key); err != nil {
			t.Fatalf("key %s: %v", key, err)
		}
	}
	if _, err := l.Allow(ctx, "c"); !errors.Is(err, limiter.ErrTooManyKeys) {
		t.Fatalf("third key: got %v, want ErrTooManyKeys", err)
	}
	// Existing keys keep working while the store is full.
	if _, err := l.Allow(ctx, "a"); err != nil {
		t.Fatalf("existing key while full: %v", err)
	}
	// Once old state expires, new keys can reclaim the space even before
	// the janitor runs.
	clk.Advance(time.Minute)
	if _, err := l.Allow(ctx, "c"); err != nil {
		t.Fatalf("new key after expiry: %v", err)
	}
}

func TestRunJanitorStopsOnCancel(t *testing.T) {
	sweeps := make(chan int, 1)
	l, _ := limiter.NewFixedWindow(1, time.Minute, limiter.Options{})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		limiter.RunJanitor(ctx, time.Millisecond, l, func(removed int) {
			select {
			case sweeps <- removed:
			default:
			}
		})
		close(done)
	}()

	select {
	case <-sweeps:
	case <-time.After(5 * time.Second):
		t.Fatal("janitor never swept")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("janitor did not stop after cancel")
	}
}
