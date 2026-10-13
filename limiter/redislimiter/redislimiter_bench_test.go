package redislimiter

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/popeyethecode/throttlex/limiter"
)

// BenchmarkRedis measures the Redis limiters against a real Redis server.
// Numbers from miniredis would say nothing about real network round trips,
// so this benchmark is skipped unless THROTTLEX_TEST_REDIS_ADDR is set:
//
//	docker compose up -d redis
//	THROTTLEX_TEST_REDIS_ADDR=localhost:6379 go test ./limiter/redislimiter -run '^$' -bench . -benchmem
func BenchmarkRedis(b *testing.B) {
	addr := os.Getenv("THROTTLEX_TEST_REDIS_ADDR")
	if addr == "" {
		b.Skip("set THROTTLEX_TEST_REDIS_ADDR to benchmark against a real Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	opts := Options{Prefix: "bench:" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":", Timeout: time.Second}

	fw, _ := NewFixedWindow(client, 100, time.Minute, opts)
	tb, _ := NewTokenBucket(client, 100, 100.0/60, opts)
	keys := make([]string, 10_000)
	for i := range keys {
		keys[i] = "client-" + strconv.Itoa(i)
	}

	for name, l := range map[string]limiter.Limiter{"fixed_window": fw, "token_bucket": tb} {
		b.Run(name+"/parallel_10k_keys", func(b *testing.B) {
			var worker atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := int(worker.Add(1)) * 7919
				for pb.Next() {
					if _, err := l.Allow(context.Background(), keys[i%len(keys)]); err != nil {
						b.Error(err)
						return
					}
					i++
				}
			})
		})
	}
}
