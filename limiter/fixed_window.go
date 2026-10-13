package limiter

import (
	"context"
	"time"
)

// FixedWindow allows up to limit requests per key in each fixed window of
// time, e.g. 100 requests in every calendar minute 12:00:00-12:00:59.
//
// It is the cheapest algorithm (one counter per key) but suffers from the
// boundary-burst problem: a client can send limit requests at 12:00:59 and
// another limit requests at 12:01:00, i.e. 2x the limit within one second,
// because the two bursts fall in different windows.
type FixedWindow struct {
	*store[fixedWindowState] // embedded: FixedWindow gets Sweep and Len for free
	limit                    int
	window                   time.Duration
}

type fixedWindowState struct {
	windowStart int64 // Unix nanoseconds
	count       int
}

// NewFixedWindow returns a FixedWindow limiter.
func NewFixedWindow(limit int, window time.Duration, opts Options) (*FixedWindow, error) {
	if err := validateLimitWindow(limit, window); err != nil {
		return nil, err
	}
	opts, err := opts.normalize()
	if err != nil {
		return nil, err
	}
	return &FixedWindow{
		store:  newStore[fixedWindowState](opts.Shards, opts.MaxKeys, opts.Clock),
		limit:  limit,
		window: window,
	}, nil
}

// Allow implements Limiter.
func (l *FixedWindow) Allow(_ context.Context, key string) (Decision, error) {
	return l.update(key, func(st *fixedWindowState, now time.Time) (Decision, time.Time) {
		// Windows are aligned to multiples of l.window since the Unix epoch,
		// so every key shares the same window boundaries. (Integer math on
		// Unix nanoseconds benchmarked faster than time.Truncate.)
		nowNs := now.UnixNano()
		start := windowStart(nowNs, l.window)
		if st.windowStart != start {
			st.windowStart = start
			st.count = 0
		}
		end := start + int64(l.window)
		untilEnd := time.Duration(end - nowNs)

		d := Decision{Limit: l.limit, ResetAfter: untilEnd}
		if st.count < l.limit {
			st.count++
			d.Allowed = true
		} else {
			d.RetryAfter = untilEnd
		}
		d.Remaining = l.limit - st.count
		return d, time.Unix(0, end)
	})
}
