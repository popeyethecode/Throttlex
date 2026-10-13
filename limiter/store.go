package limiter

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

// numShards is how many independently locked maps a store uses.
//
// Locking design, from simplest to most scalable:
//   - one global mutex: simplest, but every request in the process waits
//     on the same lock, even for unrelated clients.
//   - one mutex per key: no contention between clients, but creating and
//     deleting per-key mutexes safely is subtle (cleanup can race with a
//     request that just fetched the mutex).
//   - sharded locks (used here): keys are spread across N maps, each with
//     its own mutex. Unrelated clients rarely share a lock, and cleanup is
//     simple because the shard lock protects both the map and the state.
//
// The benchmarks in limiter_bench_test.go compare 1 shard against 64.
const numShards = 64

// store holds per-key algorithm state of type S with bounded size and
// expiry. It is generic so all four algorithms share one correct,
// concurrency-safe implementation.
type store[S any] struct {
	shards  []shard[S]
	seed    maphash.Seed
	clock   Clock
	maxKeys int64
	count   atomic.Int64 // number of keys currently stored, across all shards
}

type shard[S any] struct {
	mu      sync.Mutex
	entries map[string]*entry[S]
}

type entry[S any] struct {
	state S
	// expiresAt is when the state becomes equivalent to a brand-new key.
	// From then on, deleting the entry cannot change any future decision.
	expiresAt time.Time
}

func newStore[S any](shards int, maxKeys int, clock Clock) *store[S] {
	s := &store[S]{
		shards:  make([]shard[S], shards),
		seed:    maphash.MakeSeed(), // random per process, so attackers cannot aim keys at one shard
		clock:   clock,
		maxKeys: int64(maxKeys),
	}
	for i := range s.shards {
		s.shards[i].entries = make(map[string]*entry[S])
	}
	return s
}

// update runs fn on the state for key while holding that key's shard lock,
// so the read-modify-write in fn is atomic with respect to other requests
// and to cleanup. fn receives the current time and returns the decision and
// the time at which the state expires.
//
// The time is read inside the lock so that, for any one key, fn always sees
// non-decreasing timestamps even when goroutines race.
func (s *store[S]) update(key string, fn func(state *S, now time.Time) (Decision, time.Time)) (Decision, error) {
	if key == "" {
		return Decision{}, ErrEmptyKey
	}
	sh := &s.shards[maphash.String(s.seed, key)%uint64(len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := s.clock.Now()
	e, ok := sh.entries[key]
	switch {
	case ok && !now.Before(e.expiresAt):
		// Expired but not yet cleaned up: treat it exactly like a new key.
		var zero S
		e.state = zero
	case !ok:
		if !s.reserve() {
			s.sweepShardLocked(sh, now) // free space if this shard has expired keys
			if !s.reserve() {
				return Decision{}, ErrTooManyKeys
			}
		}
		e = &entry[S]{}
		sh.entries[key] = e
	}

	d, expiresAt := fn(&e.state, now)
	e.expiresAt = expiresAt
	return d, nil
}

// reserve claims room for one more key, failing if the store is full.
// Add-then-check keeps the bound exact even when shards race.
func (s *store[S]) reserve() bool {
	if s.count.Add(1) > s.maxKeys {
		s.count.Add(-1)
		return false
	}
	return true
}

// sweepShardLocked deletes expired entries. The caller must hold sh.mu.
func (s *store[S]) sweepShardLocked(sh *shard[S], now time.Time) int {
	removed := 0
	for key, e := range sh.entries {
		if !now.Before(e.expiresAt) {
			delete(sh.entries, key)
			removed++
		}
	}
	s.count.Add(-int64(removed))
	return removed
}

// Sweep removes every expired entry and returns how many were removed.
// It locks one shard at a time, so requests for keys in other shards are
// never blocked by cleanup.
func (s *store[S]) Sweep() int {
	removed := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		removed += s.sweepShardLocked(sh, s.clock.Now())
		sh.mu.Unlock()
	}
	return removed
}

// Len returns the number of keys currently tracked.
func (s *store[S]) Len() int { return int(s.count.Load()) }

// Sweeper is implemented by every in-memory limiter.
type Sweeper interface {
	Sweep() int
	Len() int
}

// RunJanitor calls s.Sweep every interval until ctx is cancelled.
// It blocks, so start it with `go`. onSweep, if not nil, is called with the
// number of removed keys after each sweep.
func RunJanitor(ctx context.Context, interval time.Duration, s Sweeper, onSweep func(removed int)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed := s.Sweep()
			if onSweep != nil {
				onSweep(removed)
			}
		}
	}
}
