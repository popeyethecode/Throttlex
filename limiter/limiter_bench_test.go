package limiter_test

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/popeyethecode/throttlex/limiter"
)

// Run with, for example:
//
//	go test ./limiter -run=^$ -bench=. -benchmem -cpu=1,4,12
//
// -cpu runs every benchmark once per GOMAXPROCS value, which is how we
// measure throughput as the number of parallel workers grows.

const benchLimit = 100

func newBenchLimiter(b *testing.B, name string, opts limiter.Options) limiter.Limiter {
	b.Helper()
	var l limiter.Limiter
	var err error
	switch name {
	case "fixed_window":
		l, err = limiter.NewFixedWindow(benchLimit, time.Minute, opts)
	case "sliding_log":
		l, err = limiter.NewSlidingLog(benchLimit, time.Minute, opts)
	case "sliding_counter":
		l, err = limiter.NewSlidingCounter(benchLimit, time.Minute, opts)
	case "token_bucket":
		l, err = limiter.NewTokenBucket(benchLimit, benchLimit/60.0, opts)
	}
	if err != nil {
		b.Fatal(err)
	}
	return l
}

var benchAlgorithms = []string{"fixed_window", "sliding_log", "sliding_counter", "token_bucket"}

func makeKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "client-" + strconv.Itoa(i)
	}
	return keys
}

// BenchmarkAllow compares the algorithms under three workloads:
//   - serial: one goroutine, one key (raw algorithm cost)
//   - parallel_hot_key: every goroutine hits the same key (worst-case lock contention)
//   - parallel_10k_keys: goroutines spread over 10,000 keys (typical multi-client load)
//
// With a hot key, most calls are denials after the first 100, which is the
// realistic case for a client that is being throttled.
func BenchmarkAllow(b *testing.B) {
	ctx := context.Background()
	keys := makeKeys(10_000)

	for _, algo := range benchAlgorithms {
		b.Run(algo+"/serial", func(b *testing.B) {
			l := newBenchLimiter(b, algo, limiter.Options{})
			b.ReportAllocs()
			for b.Loop() {
				l.Allow(ctx, "hot")
			}
		})
		b.Run(algo+"/parallel_hot_key", func(b *testing.B) {
			l := newBenchLimiter(b, algo, limiter.Options{})
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l.Allow(ctx, "hot")
				}
			})
		})
		b.Run(algo+"/parallel_10k_keys", func(b *testing.B) {
			l := newBenchLimiter(b, algo, limiter.Options{})
			var worker atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := int(worker.Add(1)) * 7919 // each goroutine starts at a different key
				for pb.Next() {
					l.Allow(ctx, keys[i%len(keys)])
					i++
				}
			})
		})
	}
}

// BenchmarkShards measures whether lock sharding pays off: one mutex for
// every key versus 64 independently locked shards, many keys, in parallel.
func BenchmarkShards(b *testing.B) {
	ctx := context.Background()
	keys := makeKeys(10_000)
	for _, shards := range []int{1, 64} {
		b.Run("shards="+strconv.Itoa(shards), func(b *testing.B) {
			l := newBenchLimiter(b, "token_bucket", limiter.Options{Shards: shards})
			var worker atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := int(worker.Add(1)) * 7919
				for pb.Next() {
					l.Allow(ctx, keys[i%len(keys)])
					i++
				}
			})
		})
	}
}

// BenchmarkMemoryPerKey reports the heap used per tracked client, measured
// by tracking 100,000 distinct keys (key strings themselves are allocated
// beforehand and not counted). The sliding log allocates its full ring
// buffer (benchLimit x 8 bytes) on a key's first request.
func BenchmarkMemoryPerKey(b *testing.B) {
	const n = 100_000
	ctx := context.Background()
	keys := makeKeys(n)

	for _, algo := range benchAlgorithms {
		b.Run(algo, func(b *testing.B) {
			for b.Loop() {
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)

				l := newBenchLimiter(b, algo, limiter.Options{MaxKeys: n})
				for _, k := range keys {
					l.Allow(ctx, k)
				}

				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/n, "bytes/key")
				runtime.KeepAlive(l)
			}
		})
	}
}
