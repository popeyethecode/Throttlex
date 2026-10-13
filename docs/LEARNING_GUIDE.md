# Learning guide: reading ThrottleX as a Go beginner

Read the files in this order. Each step introduces a few Go concepts and points to where they appear.

## 1. Packages, structs, errors: `internal/config/config.go`

- **Package**: one folder is one package. Names starting with an uppercase letter (`Load`, `Config`) are exported; lowercase names (`parser`) are private to the package.
- **Struct**: `Config` groups related fields. `Config{Addr: ":8080"}` creates one.
- **Errors are values**: `Load` returns `(Config, error)`, and callers check `if err != nil`. `fmt.Errorf("...: %w", err)` wraps an error with context. `errors.Join` combines several errors, so every config problem is reported at once.
- **Functions as values**: `Load(getenv func(string) string)` takes a function, which lets tests pass a fake environment.
- **Methods with pointer receivers**: `func (p *parser) fail(...)` can modify `p.errs` because `p` is a pointer.

Try it: run `go test ./internal/config -v` and read the table-driven tests in `config_test.go`.

## 2. Interfaces: `limiter/limiter.go`, `limiter/clock.go`

- **Interface**: `Limiter` has a single method, `Allow`. Any type with that method *is* a `Limiter` automatically; there is no `implements` keyword.
- **Why `Clock` is an interface**: production uses `RealClock` (`time.Now`), and tests use `ManualClock`, which only moves when the test calls `Advance`. That is how the time tests are deterministic, with no `time.Sleep`.
- **Sentinel errors**: `ErrTooManyKeys` is compared with `errors.Is`, which also sees through wrapping.

## 3. Mutexes, generics, embedding: `limiter/store.go`

- **`sync.Mutex`**: `Lock()`/`Unlock()` ensure only one goroutine touches a shard at a time. `defer sh.mu.Unlock()` guarantees the unlock even on early return.
- **`sync/atomic`**: `atomic.Int64` updates one number safely without a mutex (the key counter).
- **Generics**: `store[S any]` works for any state type `S`. Each algorithm stores its own state struct in the same safe container.
- **Embedding**: `FixedWindow` embeds `*store[fixedWindowState]`, so `FixedWindow` gets the `Sweep` and `Len` methods "for free".
- **Closures**: `update(key, func(st *S, now time.Time) ...)` passes in a function that runs *while the lock is held*, which makes the read-modify-write atomic.

## 4. The algorithms: `fixed_window.go`, `sliding_log.go`, `sliding_counter.go`, `token_bucket.go`

Read the comment at the top of each file first, then `limiter_test.go`. Each test is a script of steps: "advance the clock by X, send a request, expect allowed/remaining/retry". Change a number in a test and watch it fail. That's a good way to check your understanding.

## 5. Goroutines, channels, `select`, `context`: `limiter/store.go` (`RunJanitor`), `cmd/server/main.go`

- **Goroutine**: `go f()` runs `f` concurrently. `net/http` runs every request in its own goroutine, which is why all limiter state must be locked.
- **Channel**: `errCh := make(chan error, 1)`. One goroutine sends (`errCh <- err`) and another receives (`<-errCh`).
- **`select`**: waits for whichever channel is ready first, for example "server crashed" vs "Ctrl+C pressed", or "ticker fired" vs "context cancelled".
- **`context.Context`**: a cancellation signal passed down through calls. `signal.NotifyContext` cancels it on Ctrl+C. `context.WithTimeout` bounds Redis calls.
- **`sync.WaitGroup`**: `main` waits for the janitor goroutine to finish before exiting, which is what makes shutdown predictable.

## 6. HTTP and middleware: `internal/middleware/ratelimit.go`, `internal/server/server.go`

- **`http.Handler`**: an interface with one method, `ServeHTTP(w, r)`.
- **Middleware**: `func(http.Handler) http.Handler`. It returns a new handler that runs code *before* (or instead of) calling the next one. `RateLimit` returns 429 and never calls `next` when a client is over its limit.
- **Routing**: Go 1.22+ patterns like `"GET /api/demo"` return 405 for wrong methods automatically.
- **`httptest`**: `NewRecorder` calls a handler directly with no network, and `NewServer` starts a real server on a random port.

## 7. Distributed state: `limiter/redislimiter/redislimiter.go`

- Why a Go mutex is not enough across machines, and how a Lua script makes "read, decide, write" atomic inside Redis.
- How `ctx` with a timeout prevents a slow Redis from hanging requests, and how the middleware's fail-open/fail-closed policy decides what happens next.

## Useful commands

```bash
go test ./... -v                         # all tests, verbose
go test ./limiter -run TestTokenBucket -v  # one test
go test ./... -cover                     # coverage percentage
go test -race ./...                      # data race detector (needs cgo; easiest on Linux/WSL)
go test ./limiter -run '^$' -bench . -benchmem   # benchmarks
go doc ./limiter Limiter                 # read documentation from the terminal
```
