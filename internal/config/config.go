// Package config loads and validates ThrottleX settings from environment
// variables. Every variable is documented in .env.example and the README.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/popeyethecode/throttlex/internal/identity"
	"github.com/popeyethecode/throttlex/limiter"
)

// Algorithm names.
const (
	FixedWindow    = "fixed_window"
	SlidingLog     = "sliding_log"
	SlidingCounter = "sliding_counter"
	TokenBucket    = "token_bucket"
)

// Backend names.
const (
	BackendMemory = "memory"
	BackendRedis  = "redis"
)

// Config holds every setting the service needs at startup.
type Config struct {
	Addr     string
	LogLevel slog.Level

	Backend   string
	Algorithm string
	// Limit and Window configure the window algorithms.
	Limit  int
	Window time.Duration
	// BucketCapacity and RefillPerSecond configure the token bucket.
	BucketCapacity  int
	RefillPerSecond float64

	Identity       []identity.Strategy
	APIKeyHeader   string
	UserHeader     string
	TrustedProxies []netip.Prefix

	MaxKeys         int
	CleanupInterval time.Duration

	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	RedisTimeout   time.Duration
	RedisKeyPrefix string
	FailOpen       bool

	// AdminToken enables the admin endpoints when non-empty.
	AdminToken string
}

// Load builds a Config, reading variables through getenv. In production,
// pass os.Getenv. In tests, pass a fake so tests never touch the real
// environment. All problems are reported together, not just the first.
func Load(getenv func(string) string) (Config, error) {
	p := &parser{getenv: getenv}
	cfg := Config{
		Addr:            p.str("THROTTLEX_ADDR", ":8080"),
		LogLevel:        p.logLevel("THROTTLEX_LOG_LEVEL", slog.LevelInfo),
		Backend:         strings.ToLower(p.str("THROTTLEX_BACKEND", BackendMemory)),
		Algorithm:       strings.ToLower(p.str("THROTTLEX_ALGORITHM", TokenBucket)),
		Limit:           p.int("THROTTLEX_LIMIT", 100),
		Window:          p.duration("THROTTLEX_WINDOW", time.Minute),
		APIKeyHeader:    p.str("THROTTLEX_API_KEY_HEADER", "X-API-Key"),
		UserHeader:      p.str("THROTTLEX_USER_HEADER", "X-User-ID"),
		MaxKeys:         p.int("THROTTLEX_MAX_KEYS", 100_000),
		CleanupInterval: p.duration("THROTTLEX_CLEANUP_INTERVAL", time.Minute),
		RedisAddr:       p.str("THROTTLEX_REDIS_ADDR", "localhost:6379"),
		RedisPassword:   p.str("THROTTLEX_REDIS_PASSWORD", ""),
		RedisDB:         p.int("THROTTLEX_REDIS_DB", 0),
		RedisTimeout:    p.duration("THROTTLEX_REDIS_TIMEOUT", 100*time.Millisecond),
		RedisKeyPrefix:  p.str("THROTTLEX_REDIS_KEY_PREFIX", "throttlex:"),
		AdminToken:      p.str("THROTTLEX_ADMIN_TOKEN", ""),
	}
	// The token bucket defaults to the same average rate as the window
	// algorithms: Limit requests per Window, with bursts of up to Limit.
	cfg.BucketCapacity = p.int("THROTTLEX_BUCKET_CAPACITY", cfg.Limit)
	defaultRate := 0.0
	if cfg.Window > 0 {
		defaultRate = float64(cfg.Limit) / cfg.Window.Seconds()
	}
	cfg.RefillPerSecond = p.float("THROTTLEX_REFILL_RATE", defaultRate)

	switch policy := strings.ToLower(p.str("THROTTLEX_FAILURE_POLICY", "open")); policy {
	case "open":
		cfg.FailOpen = true
	case "closed":
		cfg.FailOpen = false
	default:
		p.fail("THROTTLEX_FAILURE_POLICY must be open or closed, got %q", policy)
	}

	strategies, err := identity.ParseStrategies(p.str("THROTTLEX_IDENTITY", "apikey,user,ip"))
	if err != nil {
		p.fail("THROTTLEX_IDENTITY: %v", err)
	}
	cfg.Identity = strategies

	if v := p.str("THROTTLEX_TRUSTED_PROXIES", ""); v != "" {
		for _, s := range strings.Split(v, ",") {
			prefix, err := netip.ParsePrefix(strings.TrimSpace(s))
			if err != nil {
				p.fail("THROTTLEX_TRUSTED_PROXIES: %q is not a CIDR like 10.0.0.0/8", s)
				continue
			}
			cfg.TrustedProxies = append(cfg.TrustedProxies, prefix.Masked())
		}
	}

	cfg.validate(p)
	if len(p.errs) > 0 {
		return Config{}, errors.Join(p.errs...)
	}
	return cfg, nil
}

func (c *Config) validate(p *parser) {
	if _, port, err := net.SplitHostPort(c.Addr); err != nil {
		p.fail("THROTTLEX_ADDR %q is not host:port: %v", c.Addr, err)
	} else if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		p.fail("THROTTLEX_ADDR %q: port must be a number between 1 and 65535", c.Addr)
	}

	switch c.Algorithm {
	case FixedWindow, SlidingLog, SlidingCounter, TokenBucket:
	default:
		p.fail("THROTTLEX_ALGORITHM must be one of %s, %s, %s, %s; got %q",
			FixedWindow, SlidingLog, SlidingCounter, TokenBucket, c.Algorithm)
	}
	switch c.Backend {
	case BackendMemory:
	case BackendRedis:
		// Only algorithms with a tested Redis implementation are accepted.
		if c.Algorithm != FixedWindow && c.Algorithm != TokenBucket {
			p.fail("THROTTLEX_BACKEND=redis supports only %s and %s, got %q", FixedWindow, TokenBucket, c.Algorithm)
		}
		if c.RedisAddr == "" {
			p.fail("THROTTLEX_REDIS_ADDR is required when THROTTLEX_BACKEND=redis")
		}
	default:
		p.fail("THROTTLEX_BACKEND must be memory or redis, got %q", c.Backend)
	}

	if c.Limit < 1 || c.Limit > 1_000_000_000 {
		p.fail("THROTTLEX_LIMIT must be between 1 and 1000000000, got %d", c.Limit)
	}
	if c.Algorithm == SlidingLog && c.Limit > limiter.MaxSlidingLogLimit {
		p.fail("THROTTLEX_LIMIT must be at most %d for sliding_log (memory grows with the limit)", limiter.MaxSlidingLogLimit)
	}
	if c.Window < time.Millisecond {
		p.fail("THROTTLEX_WINDOW must be at least 1ms, got %s", c.Window)
	}
	if c.BucketCapacity < 1 || c.BucketCapacity > 1_000_000_000 {
		p.fail("THROTTLEX_BUCKET_CAPACITY must be between 1 and 1000000000, got %d", c.BucketCapacity)
	}
	if !(c.RefillPerSecond > 0) || math.IsInf(c.RefillPerSecond, 0) {
		p.fail("THROTTLEX_REFILL_RATE must be a positive number, got %v", c.RefillPerSecond)
	}
	if c.MaxKeys < 1 || c.MaxKeys > 10_000_000 {
		p.fail("THROTTLEX_MAX_KEYS must be between 1 and 10000000, got %d", c.MaxKeys)
	}
	if c.CleanupInterval < time.Second {
		p.fail("THROTTLEX_CLEANUP_INTERVAL must be at least 1s, got %s", c.CleanupInterval)
	}
	if c.RedisTimeout <= 0 {
		p.fail("THROTTLEX_REDIS_TIMEOUT must be positive, got %s", c.RedisTimeout)
	}
	if c.RedisDB < 0 {
		p.fail("THROTTLEX_REDIS_DB must not be negative, got %d", c.RedisDB)
	}
	if c.APIKeyHeader == "" || c.UserHeader == "" {
		p.fail("THROTTLEX_API_KEY_HEADER and THROTTLEX_USER_HEADER must not be empty")
	}
	if c.AdminToken != "" && len(c.AdminToken) < 16 {
		p.fail("THROTTLEX_ADMIN_TOKEN must be at least 16 characters (or empty to disable admin endpoints)")
	}
}

// parser reads typed values and collects every error instead of stopping
// at the first, so a misconfigured deployment can be fixed in one pass.
type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) fail(format string, args ...any) {
	p.errs = append(p.errs, fmt.Errorf(format, args...))
}

func (p *parser) str(key, def string) string {
	if v := strings.TrimSpace(p.getenv(key)); v != "" {
		return v
	}
	return def
}

func (p *parser) int(key string, def int) int {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail("%s must be an integer, got %q", key, v)
		return def
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.fail("%s must be a number, got %q", key, v)
		return def
	}
	return f
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail("%s must be a duration like 500ms, 30s or 1m, got %q", key, v)
		return def
	}
	return d
}

func (p *parser) logLevel(key string, def slog.Level) slog.Level {
	switch v := strings.ToLower(p.str(key, "")); v {
	case "":
		return def
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		p.fail("%s must be debug, info, warn, or error, got %q", key, v)
		return def
	}
}
