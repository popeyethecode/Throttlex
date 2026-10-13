package limiter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/popeyethecode/throttlex/limiter"
)

// t0 is aligned to a whole minute, so window boundaries are easy to reason about.
var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// step is one request in a scripted scenario.
type step struct {
	advance   time.Duration // move the clock forward before the request
	key       string        // defaults to "client"
	allowed   bool
	remaining int
	retry     time.Duration // expected RetryAfter (always 0 when allowed)
}

// runSteps replays steps against l, failing on the first mismatch.
func runSteps(t *testing.T, l limiter.Limiter, clk *limiter.ManualClock, steps []step) {
	t.Helper()
	for i, s := range steps {
		clk.Advance(s.advance)
		key := s.key
		if key == "" {
			key = "client"
		}
		d, err := l.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("step %d: unexpected error: %v", i, err)
		}
		if d.Allowed != s.allowed || d.Remaining != s.remaining || d.RetryAfter != s.retry {
			t.Fatalf("step %d (t=%s, key=%s): got allowed=%v remaining=%d retry=%s; want allowed=%v remaining=%d retry=%s",
				i, clk.Now().Sub(t0), key, d.Allowed, d.Remaining, d.RetryAfter, s.allowed, s.remaining, s.retry)
		}
	}
}

func TestFixedWindow(t *testing.T) {
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "allows up to the limit, then denies until the window ends",
			steps: []step{
				{allowed: true, remaining: 2},
				{allowed: true, remaining: 1},
				{allowed: true, remaining: 0}, // exactly reaches the limit
				{allowed: false, remaining: 0, retry: time.Minute},
				{advance: 30 * time.Second, allowed: false, remaining: 0, retry: 30 * time.Second},
				{advance: 30 * time.Second, allowed: true, remaining: 2}, // new window
			},
		},
		{
			name: "clients have independent quotas",
			steps: []step{
				{key: "a", allowed: true, remaining: 2},
				{key: "a", allowed: true, remaining: 1},
				{key: "a", allowed: true, remaining: 0},
				{key: "a", allowed: false, remaining: 0, retry: time.Minute},
				{key: "b", allowed: true, remaining: 2},
			},
		},
		{
			name: "boundary burst: 2x the limit within one second",
			steps: []step{
				{advance: 59 * time.Second, allowed: true, remaining: 2},
				{allowed: true, remaining: 1},
				{allowed: true, remaining: 0},
				{advance: time.Second, allowed: true, remaining: 2}, // window changed at 12:01:00
				{allowed: true, remaining: 1},
				{allowed: true, remaining: 0},
			},
		},
		{
			name: "idle for several windows resets the count",
			steps: []step{
				{allowed: true, remaining: 2},
				{advance: 5 * time.Minute, allowed: true, remaining: 2},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := limiter.NewManualClock(t0)
			l, err := limiter.NewFixedWindow(3, time.Minute, limiter.Options{Clock: clk})
			if err != nil {
				t.Fatal(err)
			}
			runSteps(t, l, clk, tc.steps)
		})
	}
}

func TestSlidingLog(t *testing.T) {
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "window rolls with the clock",
			steps: []step{
				{allowed: true, remaining: 2},                            // t=0
				{advance: 10 * time.Second, allowed: true, remaining: 1}, // t=10s
				{advance: 10 * time.Second, allowed: true, remaining: 0}, // t=20s
				{advance: 10 * time.Second, allowed: false, remaining: 0, retry: 30 * time.Second},
				{advance: 30 * time.Second, allowed: true, remaining: 0}, // t=60s: the t=0 entry has left
				{allowed: false, remaining: 0, retry: 10 * time.Second},  // next to leave is t=10s
			},
		},
		{
			name: "no boundary burst",
			steps: []step{
				{advance: 59 * time.Second, allowed: true, remaining: 2},
				{allowed: true, remaining: 1},
				{allowed: true, remaining: 0},
				{advance: time.Second, allowed: false, remaining: 0, retry: 59 * time.Second},
			},
		},
		{
			name: "a timestamp exactly one window old is outside the window",
			steps: []step{
				{allowed: true, remaining: 2},
				{allowed: true, remaining: 1},
				{allowed: true, remaining: 0},
				{advance: time.Minute - time.Nanosecond, allowed: false, remaining: 0, retry: time.Nanosecond},
				{advance: time.Nanosecond, allowed: true, remaining: 2}, // all three left; one added
			},
		},
		{
			name: "clients have independent quotas",
			steps: []step{
				{key: "a", allowed: true, remaining: 2},
				{key: "a", allowed: true, remaining: 1},
				{key: "a", allowed: true, remaining: 0},
				{key: "b", allowed: true, remaining: 2},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := limiter.NewManualClock(t0)
			l, err := limiter.NewSlidingLog(3, time.Minute, limiter.Options{Clock: clk})
			if err != nil {
				t.Fatal(err)
			}
			runSteps(t, l, clk, tc.steps)
		})
	}
}

func TestSlidingCounter(t *testing.T) {
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "weights the previous window by the remaining overlap",
			steps: []step{
				{allowed: true, remaining: 3},
				{allowed: true, remaining: 2},
				{allowed: true, remaining: 1},
				{allowed: true, remaining: 0},
				// prev=0, curr=4: must wait for next window, until 4*(1-f)+1 <= 4, i.e. f=0.25.
				{allowed: false, remaining: 0, retry: 75 * time.Second},
				// t=60s: prev=4 at full weight; need weight 0.75, i.e. 15s into the window.
				{advance: 60 * time.Second, allowed: false, remaining: 0, retry: 15 * time.Second},
				{advance: 15 * time.Second, allowed: true, remaining: 0}, // 4*0.75 + 1 = 4
				{advance: 15 * time.Second, allowed: true, remaining: 0}, // 4*0.5 + 1 + 1 = 4
				// prev=4, curr=2: need 4*(1-f) <= 1, so f=0.75 (t=105s), 15s from now.
				{allowed: false, remaining: 0, retry: 15 * time.Second},
				{advance: 15 * time.Second, allowed: true, remaining: 0},
			},
		},
		{
			name: "more than one idle window resets both counters",
			steps: []step{
				{allowed: true, remaining: 3},
				{advance: 2 * time.Minute, allowed: true, remaining: 3},
			},
		},
		{
			name: "clients have independent quotas",
			steps: []step{
				{key: "a", allowed: true, remaining: 3},
				{key: "a", allowed: true, remaining: 2},
				{key: "a", allowed: true, remaining: 1},
				{key: "a", allowed: true, remaining: 0},
				{key: "b", allowed: true, remaining: 3},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := limiter.NewManualClock(t0)
			l, err := limiter.NewSlidingCounter(4, time.Minute, limiter.Options{Clock: clk})
			if err != nil {
				t.Fatal(err)
			}
			runSteps(t, l, clk, tc.steps)
		})
	}
}

// TestSlidingCounterIsAnApproximation shows the case where the counter
// disagrees with the exact log: all of the previous window's requests came
// at its very end, but the counter assumes they were spread evenly.
func TestSlidingCounterIsAnApproximation(t *testing.T) {
	clk := limiter.NewManualClock(t0)
	opts := limiter.Options{Clock: clk}
	exact, _ := limiter.NewSlidingLog(4, time.Minute, opts)
	approx, _ := limiter.NewSlidingCounter(4, time.Minute, opts)
	ctx := context.Background()

	clk.Advance(59 * time.Second) // 4 requests at t=59s
	for range 4 {
		exact.Allow(ctx, "k")
		approx.Allow(ctx, "k")
	}

	clk.Advance(31 * time.Second) // t=90s: the true last-minute count is still 4
	e, _ := exact.Allow(ctx, "k")
	a, _ := approx.Allow(ctx, "k")
	if e.Allowed {
		t.Errorf("sliding log allowed a 5th request within one minute")
	}
	if !a.Allowed {
		t.Errorf("sliding counter estimate is 4*0.5=2, so it should allow this request")
	}
}

func TestTokenBucket(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		rate     float64
		steps    []step
	}{
		{
			name:     "burst up to capacity, then one token per second",
			capacity: 3, rate: 1,
			steps: []step{
				{allowed: true, remaining: 2},
				{allowed: true, remaining: 1},
				{allowed: true, remaining: 0}, // bucket now empty
				{allowed: false, remaining: 0, retry: time.Second},
				{advance: 500 * time.Millisecond, allowed: false, remaining: 0, retry: 500 * time.Millisecond},
				{advance: 500 * time.Millisecond, allowed: true, remaining: 0},
			},
		},
		{
			name:     "refill never exceeds capacity",
			capacity: 3, rate: 1,
			steps: []step{
				{allowed: true, remaining: 2},
				{advance: time.Hour, allowed: true, remaining: 2},
			},
		},
		{
			name:     "fractional refill rate",
			capacity: 1, rate: 0.5,
			steps: []step{
				{allowed: true, remaining: 0},
				{allowed: false, remaining: 0, retry: 2 * time.Second},
				{advance: time.Second, allowed: false, remaining: 0, retry: time.Second},
				{advance: time.Second, allowed: true, remaining: 0},
			},
		},
		{
			name:     "clients have independent buckets",
			capacity: 1, rate: 1,
			steps: []step{
				{key: "a", allowed: true, remaining: 0},
				{key: "a", allowed: false, remaining: 0, retry: time.Second},
				{key: "b", allowed: true, remaining: 0},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := limiter.NewManualClock(t0)
			l, err := limiter.NewTokenBucket(tc.capacity, tc.rate, limiter.Options{Clock: clk})
			if err != nil {
				t.Fatal(err)
			}
			runSteps(t, l, clk, tc.steps)
		})
	}
}

// algorithms builds one of each limiter with the same nominal limit, so
// generic tests can run against all four.
func algorithms(t *testing.T, limit int, opts limiter.Options) map[string]limiter.Limiter {
	t.Helper()
	fw, err1 := limiter.NewFixedWindow(limit, time.Minute, opts)
	sl, err2 := limiter.NewSlidingLog(limit, time.Minute, opts)
	sc, err3 := limiter.NewSlidingCounter(limit, time.Minute, opts)
	tb, err4 := limiter.NewTokenBucket(limit, float64(limit)/60, opts)
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		t.Fatal(err)
	}
	return map[string]limiter.Limiter{
		"fixed_window": fw, "sliding_log": sl, "sliding_counter": sc, "token_bucket": tb,
	}
}

// TestRetryAfterIsAccurate checks, for every algorithm, that waiting exactly
// RetryAfter makes the next request succeed, across awkward limits and
// starting offsets.
func TestRetryAfterIsAccurate(t *testing.T) {
	for _, limit := range []int{1, 3, 7, 100} {
		for _, offset := range []time.Duration{0, 7 * time.Second, 59*time.Second + 999*time.Millisecond} {
			for _, name := range []string{"fixed_window", "sliding_log", "sliding_counter", "token_bucket"} {
				clk := limiter.NewManualClock(t0.Add(offset)) // fresh clock per algorithm
				l := algorithms(t, limit, limiter.Options{Clock: clk})[name]
				ctx := context.Background()
				var d limiter.Decision
				for range limit + 1 {
					d, _ = l.Allow(ctx, name)
				}
				if d.Allowed || d.RetryAfter <= 0 {
					t.Fatalf("%s limit=%d: request %d should be denied with a positive RetryAfter, got %+v", name, limit, limit+1, d)
				}
				clk.Advance(d.RetryAfter)
				if d2, _ := l.Allow(ctx, name); !d2.Allowed {
					t.Errorf("%s limit=%d offset=%s: denied after waiting RetryAfter=%s: %+v", name, limit, offset, d.RetryAfter, d2)
				}
			}
		}
	}
}

func TestInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		make func() error
	}{
		{"fixed window zero limit", func() error { _, err := limiter.NewFixedWindow(0, time.Minute, limiter.Options{}); return err }},
		{"fixed window zero window", func() error { _, err := limiter.NewFixedWindow(1, 0, limiter.Options{}); return err }},
		{"sliding log negative window", func() error { _, err := limiter.NewSlidingLog(1, -time.Second, limiter.Options{}); return err }},
		{"sliding log limit too large", func() error {
			_, err := limiter.NewSlidingLog(limiter.MaxSlidingLogLimit+1, time.Minute, limiter.Options{})
			return err
		}},
		{"sliding counter negative limit", func() error { _, err := limiter.NewSlidingCounter(-1, time.Minute, limiter.Options{}); return err }},
		{"token bucket zero capacity", func() error { _, err := limiter.NewTokenBucket(0, 1, limiter.Options{}); return err }},
		{"token bucket zero rate", func() error { _, err := limiter.NewTokenBucket(1, 0, limiter.Options{}); return err }},
		{"negative max keys", func() error {
			_, err := limiter.NewFixedWindow(1, time.Minute, limiter.Options{MaxKeys: -1})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.make(); !errors.Is(err, limiter.ErrInvalidConfig) {
				t.Fatalf("got error %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestEmptyKeyIsRejected(t *testing.T) {
	for name, l := range algorithms(t, 5, limiter.Options{}) {
		if _, err := l.Allow(context.Background(), ""); !errors.Is(err, limiter.ErrEmptyKey) {
			t.Errorf("%s: got %v, want ErrEmptyKey", name, err)
		}
	}
}
