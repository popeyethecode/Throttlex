package limiter

import (
	"context"
	"fmt"
	"time"
)

// MaxSlidingLogLimit caps the limit of a SlidingLog, because memory grows
// with limit for every tracked key (8 bytes per stored timestamp).
const MaxSlidingLogLimit = 100_000

// SlidingLog records the timestamp of every allowed request and allows a
// new one only if fewer than limit timestamps fall inside the last window.
//
// It is exact: there is no boundary burst, because the window rolls
// continuously with the clock. The cost is memory: up to limit timestamps
// per key (8 bytes each), versus a couple of integers for the counters.
type SlidingLog struct {
	*store[slidingLogState]
	limit  int
	window time.Duration
}

// slidingLogState is a ring buffer of timestamps in Unix nanoseconds.
// A ring buffer never shifts or reallocates: dropping the oldest entry is
// just "head++", and the buffer is allocated once at exactly limit slots.
type slidingLogState struct {
	times []int64
	head  int // index of the oldest timestamp
	n     int // number of timestamps currently stored
}

// NewSlidingLog returns a SlidingLog limiter.
func NewSlidingLog(limit int, window time.Duration, opts Options) (*SlidingLog, error) {
	if err := validateLimitWindow(limit, window); err != nil {
		return nil, err
	}
	if limit > MaxSlidingLogLimit {
		return nil, fmt.Errorf("%w: sliding log limit must be at most %d, got %d", ErrInvalidConfig, MaxSlidingLogLimit, limit)
	}
	opts, err := opts.normalize()
	if err != nil {
		return nil, err
	}
	return &SlidingLog{
		store:  newStore[slidingLogState](opts.Shards, opts.MaxKeys, opts.Clock),
		limit:  limit,
		window: window,
	}, nil
}

// Allow implements Limiter.
func (l *SlidingLog) Allow(_ context.Context, key string) (Decision, error) {
	return l.update(key, func(st *slidingLogState, now time.Time) (Decision, time.Time) {
		if st.times == nil {
			st.times = make([]int64, l.limit)
		}
		nowNs := now.UnixNano()
		window := int64(l.window)

		// A timestamp t is inside the window while t > now-window.
		// Timestamps are stored in order, so stop at the first one still inside.
		cutoff := nowNs - window
		for st.n > 0 && st.times[st.head] <= cutoff {
			st.head = (st.head + 1) % l.limit
			st.n--
		}

		d := Decision{Limit: l.limit}
		if st.n < l.limit {
			st.times[(st.head+st.n)%l.limit] = nowNs
			st.n++
			d.Allowed = true
		} else {
			// The next slot frees up when the oldest timestamp leaves the window.
			d.RetryAfter = time.Duration(st.times[st.head] + window - nowNs)
		}
		d.Remaining = l.limit - st.n

		newest := st.times[(st.head+st.n-1)%l.limit]
		d.ResetAfter = time.Duration(newest + window - nowNs)
		return d, time.Unix(0, newest+window)
	})
}
