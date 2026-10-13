package limiter

import (
	"sync"
	"time"
)

// Clock tells a limiter what time it is.
//
// Hiding time.Now behind an interface lets tests move time forward
// instantly and deterministically instead of calling time.Sleep.
type Clock interface {
	Now() time.Time
}

// RealClock is the production Clock backed by time.Now.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// ManualClock is a Clock that only moves when told to. It is safe for
// concurrent use, so stress tests can share one across goroutines.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewManualClock returns a ManualClock set to start.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

// Now returns the clock's current time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set moves the clock to t.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
