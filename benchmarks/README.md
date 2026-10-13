# Benchmarks

All numbers below were measured, not estimated. The raw output is in [`results/`](results/).

## Environment

| | |
|---|---|
| CPU | 12th Gen Intel Core i5-12450H (8 cores / 12 threads) |
| RAM | 16 GB |
| OS | Windows 11 Home 10.0.26200 (laptop; other desktop apps were not closed) |
| Go | 1.27.2 windows/amd64 |
| Date | 2026-10-13 |
| Runs | `-count 1` (a single run per benchmark, so expect about ±10% noise; use `-count 10` and `benchstat` for publishable comparisons) |

## How to reproduce

```bash
# Algorithm, contention, sharding and memory benchmarks (about 90 s)
go test ./limiter ./internal/middleware -run '^$' -bench . -benchmem -cpu 1,4,12

# Compare two versions properly
go install golang.org/x/perf/cmd/benchstat@latest
go test ./limiter -run '^$' -bench Allow -count 10 > old.txt   # before a change
go test ./limiter -run '^$' -bench Allow -count 10 > new.txt   # after
benchstat old.txt new.txt
```

`-cpu 1,4,12` runs every benchmark with GOMAXPROCS = 1, 4 and 12. The `-N` suffix in the results is that value. For `RunParallel` benchmarks, it is also the number of worker goroutines.

### Workloads

| Name | What it does | Why |
|---|---|---|
| `serial` | 1 goroutine, 1 key | Raw algorithm cost |
| `parallel_hot_key` | N goroutines, the same key | Worst-case lock contention (one abusive client) |
| `parallel_10k_keys` | N goroutines over 10,000 keys | Typical multi-client traffic (high key cardinality) |
| `Shards` | Token bucket, 10k keys, 1 lock vs 64 locks | Justifies the sharded design |
| `MemoryPerKey` | Heap growth after tracking 100,000 keys | Memory as client count grows |
| `Middleware` | Full HTTP middleware via `httptest` (identify + Allow + headers) | Overhead added to a request, no network |

All limiters use limit 100 per minute (token bucket: capacity 100, 100/60 tokens per second) with the real clock. In the hot-key workloads, almost every call after the first 100 is a denial. That is realistic for a throttled client, and both paths take the same lock.

## Results (ns/op; lower is better; all 0 B/op, 0 allocs/op)

| Algorithm | serial | hot key, 1 / 4 / 12 workers | 10k keys, 1 / 4 / 12 workers |
|---|---|---|---|
| Fixed window | 101 | 82 / 107 / 126 | 78 / 40 / **25.7** |
| Sliding log | 75 | 68 / 116 / 143 | 84 / 46 / 30.0 |
| Sliding counter | 78 | 71 / 132 / 148 | 77 / 44 / 29.2 |
| Token bucket | 77 | 73 / 118 / 141 | 81 / 44 / 27.9 |

### Lock sharding (token bucket, 10k keys)

| Shards | 1 worker | 4 workers | 12 workers |
|---|---|---|---|
| 1 (global mutex) | 84 | 152 | 201 |
| 64 | 83 | 45 | **28.5** |

### Memory per tracked key (100,000 keys)

| Algorithm | Bytes/key |
|---|---|
| Fixed window | 83 |
| Sliding counter | 83 |
| Token bucket | 99 |
| Sliding log (limit 100) | 995 |

### HTTP middleware (in-process)

| Workers | ns/op | B/op | allocs/op |
|---|---|---|---|
| 1 | 1713 | 1096 | 15 |
| 4 | 711 | 1096 | 15 |
| 12 | 744 | 1096 | 15 |

## What the numbers say

1. **The algorithms cost about the same per call (75–100 ns).** The dominant cost is hashing the key, taking the shard lock and the map lookup, not the algorithm's arithmetic. The choice between them should be made on accuracy and memory, not speed.
2. **Many clients scale; one hot client doesn't.** With 10k keys, throughput improves roughly 3× from 1 to 12 workers, because requests land on different shards. With one hot key, all workers queue on a single mutex, and time per op *rises* with workers. That is expected and correct: one client's requests must be serialized to count them exactly.
3. **Sharding is justified by measurement.** With a single global mutex, 12 workers are about 7× slower than with 64 shards, and slower than a single worker. With 1 worker there is no difference, which is why the simplest design is fine until there is real concurrency.
4. **Memory matches the theory.** The three counter-style algorithms need about 83–99 bytes per client (map entry, key string header, small state struct). The sliding log adds 8 bytes × limit for its timestamp ring buffer, so it is 12× bigger at limit 100 and would be about 8 KB/client at limit 1,000. 100,000 tracked clients is about 8 MB for the token bucket and about 100 MB for the sliding log. That's why `THROTTLEX_MAX_KEYS` and the 100,000 cap on sliding-log limits exist.
5. **The HTTP layer, not the limiter, is the per-request cost.** The middleware takes about 0.7–1.7 µs and 15 allocations, mostly from `httptest.NewRecorder`, header maps and `strconv`. The limiter is under 10% of that.

### An optimization made because of a measurement

The first run showed the fixed window (160 ns) and sliding counter (140 ns) *slower* than the sliding log (95 ns), even though they do less work. The cause was `time.Time.Truncate`, which does general-purpose arithmetic relative to year 1. Replacing it with integer math on Unix nanoseconds (`now - now % window`) and storing `int64` instead of `time.Time` gave:

| | Before | After |
|---|---|---|
| Fixed window, serial | 160 ns/op | 101 ns/op |
| Sliding counter, serial | 140 ns/op | 78 ns/op |
| Memory/key (both) | 99 B | 83 B |

Before/after were single runs on the same machine within the same hour. The tests confirmed behavior was unchanged.

## Accuracy vs speed vs memory

A faster or smaller algorithm is not automatically better:

- **Fixed window**: cheapest, but allows up to 2× the limit across a window boundary.
- **Sliding counter**: same cost as the fixed window, much smaller boundary error, but it can over- or under-admit when traffic within a window is uneven (see `TestSlidingCounterIsAnApproximation`).
- **Sliding log**: exact, but memory grows with the limit.
- **Token bucket**: smooth average rate with controlled bursts. It answers a different question ("average rate with burst allowance") than "N per window".

## Not yet measured

These need tools or services that were not available on the benchmark machine. The commands are ready:

**HTTP latency under load** (install [`hey`](https://github.com/rakyll/hey): `go install github.com/rakyll/hey@latest`):

```bash
# Server with a limit high enough that nothing is rejected: measures pure overhead
THROTTLEX_ALGORITHM=token_bucket THROTTLEX_LIMIT=1000000000 THROTTLEX_LOG_LEVEL=warn go run ./cmd/server
hey -n 200000 -c 50 http://localhost:8080/api/demo                         # one client (hot key)
hey -n 200000 -c 50 -H "X-API-Key: key-$RANDOM" http://localhost:8080/api/demo   # repeat with different keys
```

**In-memory vs Redis** (Redis on localhost, so this measures round-trip cost, not a production network):

```bash
docker compose up -d redis
THROTTLEX_TEST_REDIS_ADDR=localhost:6379 go test ./limiter/redislimiter -run '^$' -bench . -benchmem -cpu 1,4,12
```

Each Redis call includes a network round trip plus a Lua script execution, so expect it to be orders of magnitude slower than the in-memory path. That is the price of sharing state across instances; measure it rather than assume a number. When recording these, note the Redis version, whether Redis ran in Docker, and the same machine details as above.
