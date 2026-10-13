// Command server runs the ThrottleX HTTP service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/popeyethecode/throttlex/internal/config"
	"github.com/popeyethecode/throttlex/internal/identity"
	"github.com/popeyethecode/throttlex/internal/middleware"
	"github.com/popeyethecode/throttlex/internal/observability"
	"github.com/popeyethecode/throttlex/internal/server"
	"github.com/popeyethecode/throttlex/limiter"
	"github.com/popeyethecode/throttlex/limiter/redislimiter"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

// run holds the real startup logic. Returning an error instead of calling
// os.Exit deep inside keeps every deferred cleanup working.
func run() error {
	healthcheck := flag.Bool("healthcheck", false, "probe /health on the configured address and exit (used by Docker)")
	flag.Parse()

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	if *healthcheck {
		return probeHealth(cfg.Addr)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	lim, sweeper, closeLimiter, err := buildLimiter(cfg, logger)
	if err != nil {
		return err
	}
	defer closeLimiter()

	stats := observability.NewStats()
	resolver := &identity.Resolver{
		Order:          cfg.Identity,
		APIKeyHeader:   cfg.APIKeyHeader,
		UserHeader:     cfg.UserHeader,
		TrustedProxies: cfg.TrustedProxies,
	}
	deps := server.Deps{
		RateLimit: middleware.RateLimit(middleware.Config{
			Limiter:  lim,
			Identify: resolver.Identify,
			FailOpen: cfg.FailOpen,
			Stats:    stats,
			Logger:   logger,
		}),
		Stats:      stats,
		Policy:     describePolicy(cfg),
		AdminToken: cfg.AdminToken,
	}
	if sweeper != nil {
		deps.TrackedKeys = sweeper.Len
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.NewHandler(deps),
		ReadHeaderTimeout: 5 * time.Second, // defends against slow-header (Slowloris) clients
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB
	}

	// ctx is cancelled on Ctrl+C (SIGINT) or SIGTERM (sent by Docker on stop).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The janitor removes expired per-client state in the background.
	// The WaitGroup lets shutdown wait until it has really stopped.
	var wg sync.WaitGroup
	if sweeper != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limiter.RunJanitor(ctx, cfg.CleanupInterval, sweeper, func(removed int) {
				stats.RecordSwept(removed)
				logger.Debug("swept expired keys", "removed", removed, "tracked", sweeper.Len())
			})
		}()
	}

	// ListenAndServe blocks, so it runs in its own goroutine and reports
	// its result through a channel.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("server starting", "addr", cfg.Addr, "backend", cfg.Backend, "algorithm", cfg.Algorithm)
		errCh <- srv.ListenAndServe()
	}()

	var runErr error
	select {
	case err := <-errCh:
		runErr = fmt.Errorf("listen: %w", err) // e.g. the port is already in use
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		// Stop accepting connections and let in-flight requests finish,
		// but give up after 10 seconds.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			runErr = fmt.Errorf("graceful shutdown: %w", err)
		}
	}

	stop()    // make sure the janitor's context is cancelled on every path
	wg.Wait() // and wait for it to exit before returning
	if runErr == nil {
		logger.Info("server stopped cleanly")
	}
	return runErr
}

// buildLimiter creates the configured limiter. sweeper is nil for Redis,
// where key expiry is handled by Redis TTLs instead of a janitor.
func buildLimiter(cfg config.Config, logger *slog.Logger) (lim limiter.Limiter, sweeper limiter.Sweeper, closeFn func(), err error) {
	if cfg.Backend == config.BackendRedis {
		client := redis.NewClient(&redis.Options{
			Addr:         cfg.RedisAddr,
			Password:     cfg.RedisPassword,
			DB:           cfg.RedisDB,
			DialTimeout:  cfg.RedisTimeout,
			ReadTimeout:  cfg.RedisTimeout,
			WriteTimeout: cfg.RedisTimeout,
		})
		closeFn = func() { client.Close() }

		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := client.Ping(pingCtx).Err(); err != nil {
			// Keep starting: the failure policy decides what requests see
			// until Redis comes back.
			logger.Warn("redis is not reachable at startup", "addr", cfg.RedisAddr, "error", err, "fail_open", cfg.FailOpen)
		}

		opts := redislimiter.Options{Prefix: cfg.RedisKeyPrefix, Timeout: cfg.RedisTimeout}
		switch cfg.Algorithm {
		case config.FixedWindow:
			lim, err = redislimiter.NewFixedWindow(client, cfg.Limit, cfg.Window, opts)
		case config.TokenBucket:
			lim, err = redislimiter.NewTokenBucket(client, cfg.BucketCapacity, cfg.RefillPerSecond, opts)
		}
		if err != nil {
			closeFn()
			return nil, nil, nil, err
		}
		return lim, nil, closeFn, nil
	}

	opts := limiter.Options{MaxKeys: cfg.MaxKeys}
	var mem interface {
		limiter.Limiter
		limiter.Sweeper
	}
	switch cfg.Algorithm {
	case config.FixedWindow:
		mem, err = limiter.NewFixedWindow(cfg.Limit, cfg.Window, opts)
	case config.SlidingLog:
		mem, err = limiter.NewSlidingLog(cfg.Limit, cfg.Window, opts)
	case config.SlidingCounter:
		mem, err = limiter.NewSlidingCounter(cfg.Limit, cfg.Window, opts)
	case config.TokenBucket:
		mem, err = limiter.NewTokenBucket(cfg.BucketCapacity, cfg.RefillPerSecond, opts)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	return mem, mem, func() {}, nil
}

func describePolicy(cfg config.Config) server.Policy {
	p := server.Policy{
		Backend:        cfg.Backend,
		Algorithm:      cfg.Algorithm,
		FailurePolicy:  "closed",
		TrustedProxies: len(cfg.TrustedProxies),
	}
	if cfg.FailOpen {
		p.FailurePolicy = "open"
	}
	for _, s := range cfg.Identity {
		p.Identity = append(p.Identity, string(s))
	}
	if cfg.Algorithm == config.TokenBucket {
		p.BucketCapacity = cfg.BucketCapacity
		p.RefillPerSecond = cfg.RefillPerSecond
	} else {
		p.Limit = cfg.Limit
		p.Window = cfg.Window.String()
	}
	if cfg.Backend == config.BackendMemory {
		p.MaxKeys = cfg.MaxKeys
	}
	return p
}

// probeHealth calls GET /health on the local server. Docker's HEALTHCHECK
// runs `server -healthcheck`, because the minimal runtime image has no curl.
func probeHealth(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %s", resp.Status)
	}
	return nil
}
