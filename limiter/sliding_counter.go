package limiter

import (
	"context"
	"math"
	"time"
)

// SlidingCounter approximates a rolling window using two fixed-window
// counters: the current window and the previous one.
//
//	estimate = previous * (1 - elapsedFraction) + current
//
// where elapsedFraction is how far we are into the current window (0..1).
// Example, limit 100 per minute: 15 seconds into the current window, with
// 80 requests last window and 30 so far this one, the estimate is
// 80*0.75 + 30 = 90, so 10 more requests are allowed.
//
// It is an approximation because it assumes the previous window's requests
// were spread evenly. If they were all at its very end, the true rolling
// count is higher than the estimate. In exchange it needs only two integers
// per key, no matter how large the limit is.
type SlidingCounter struct {
	*store[slidingCounterState]
	limit  int
	window time.Duration
}

type slidingCounterState struct {
	windowStart int64 // Unix nanoseconds
	curr, prev  int
}

// NewSlidingCounter returns a SlidingCounter limiter.
func NewSlidingCounter(limit int, window time.Duration, opts Options) (*SlidingCounter, error) {
	if err := validateLimitWindow(limit, window); err != nil {
		return nil, err
	}
	opts, err := opts.normalize()
	if err != nil {
		return nil, err
	}
	return &SlidingCounter{
		store:  newStore[slidingCounterState](opts.Shards, opts.MaxKeys, opts.Clock),
		limit:  limit,
		window: window,
	}, nil
}

// Allow implements Limiter.
func (l *SlidingCounter) Allow(_ context.Context, key string) (Decision, error) {
	return l.update(key, func(st *slidingCounterState, now time.Time) (Decision, time.Time) {
		nowNs := now.UnixNano()
		start := windowStart(nowNs, l.window)
		switch {
		case st.windowStart == start:
			// Still in the same window.
		case st.windowStart+int64(l.window) == start:
			// Moved into the next window: current becomes previous.
			st.prev, st.curr = st.curr, 0
			st.windowStart = start
		default:
			// More than one window has passed (or this is a new key).
			st.prev, st.curr = 0, 0
			st.windowStart = start
		}

		elapsed := time.Duration(nowNs - start)
		estimate := l.estimate(st.prev, st.curr, elapsed)

		d := Decision{Limit: l.limit}
		if estimate+1 <= float64(l.limit)+epsilon {
			st.curr++
			estimate++
			d.Allowed = true
		} else {
			d.RetryAfter = l.retryAfter(st.prev, st.curr, elapsed)
		}
		d.Remaining = max(0, int(math.Floor(float64(l.limit)-estimate+epsilon)))

		// The previous count stops mattering at the end of this window; the
		// current count stops mattering one full window after that.
		expiresAt := start + int64(l.window)
		if st.curr > 0 {
			expiresAt += int64(l.window)
		}
		d.ResetAfter = time.Duration(expiresAt - nowNs)
		return d, time.Unix(0, expiresAt)
	})
}

func (l *SlidingCounter) estimate(prev, curr int, elapsed time.Duration) float64 {
	prevWeight := 1 - float64(elapsed)/float64(l.window)
	return float64(prev)*prevWeight + float64(curr)
}

// retryAfter returns how long until a request would be allowed, assuming
// no other requests arrive meanwhile. It solves the estimate formula for
// the elapsed time at which estimate+1 <= limit.
func (l *SlidingCounter) retryAfter(prev, curr int, elapsed time.Duration) time.Duration {
	w := float64(l.window)
	room := float64(l.limit - 1 - curr) // what the previous window may still contribute
	if room > 0 && prev > 0 {
		// Wait within this window until prev*(1-f) <= room.
		f := 1 - room/float64(prev)
		return ceilNanos(f*w - float64(elapsed))
	}
	// Wait for the next window, where the current count becomes "previous".
	toNext := w - float64(elapsed)
	f := 0.0
	if curr > 0 {
		f = max(0, 1-float64(l.limit-1)/float64(curr))
	}
	return ceilNanos(toNext + f*w)
}
