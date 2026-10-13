// Package observability holds the service's operational counters.
package observability

import (
	"sync/atomic"
	"time"
)

// Stats counts rate-limit outcomes. Its methods are safe to call from many
// goroutines at once: atomic counters need no mutex for a single number.
type Stats struct {
	start    time.Time
	allowed  atomic.Int64
	denied   atomic.Int64
	errors   atomic.Int64
	failOpen atomic.Int64
	swept    atomic.Int64
}

// NewStats returns zeroed counters, with uptime measured from now.
func NewStats() *Stats { return &Stats{start: time.Now()} }

func (s *Stats) RecordAllowed()    { s.allowed.Add(1) }
func (s *Stats) RecordDenied()     { s.denied.Add(1) }
func (s *Stats) RecordError()      { s.errors.Add(1) }
func (s *Stats) RecordFailOpen()   { s.failOpen.Add(1) }
func (s *Stats) RecordSwept(n int) { s.swept.Add(int64(n)) }

// Snapshot is a point-in-time copy of the counters, ready for JSON.
type Snapshot struct {
	UptimeSeconds   int64 `json:"uptime_seconds"`
	Allowed         int64 `json:"requests_allowed"`
	Denied          int64 `json:"requests_denied"`
	LimiterErrors   int64 `json:"limiter_errors"`
	FailOpenAllowed int64 `json:"fail_open_allowed"`
	KeysSwept       int64 `json:"keys_swept"`
}

// Snapshot returns the current values.
func (s *Stats) Snapshot() Snapshot {
	return Snapshot{
		UptimeSeconds:   int64(time.Since(s.start).Seconds()),
		Allowed:         s.allowed.Load(),
		Denied:          s.denied.Load(),
		LimiterErrors:   s.errors.Load(),
		FailOpenAllowed: s.failOpen.Load(),
		KeysSwept:       s.swept.Load(),
	}
}
