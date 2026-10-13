// Package middleware provides HTTP middleware that enforces rate limits.
package middleware

import (
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/popeyethecode/throttlex/internal/httpjson"
	"github.com/popeyethecode/throttlex/internal/observability"
	"github.com/popeyethecode/throttlex/limiter"
)

// Config configures RateLimit.
type Config struct {
	// Limiter makes the allow/deny decision.
	Limiter limiter.Limiter
	// Identify returns the rate-limit key for a request.
	Identify func(*http.Request) (string, error)
	// FailOpen decides what happens when the limiter itself fails, e.g.
	// Redis is down: true lets requests through (availability first),
	// false rejects them with 503 (protection first).
	FailOpen bool
	// Stats, if not nil, receives outcome counts.
	Stats *observability.Stats
	// Logger, if nil, defaults to slog.Default().
	Logger *slog.Logger
}

// RateLimit returns middleware that rate-limits every request to the
// wrapped handler.
//
// A middleware in Go is just a function that takes an http.Handler and
// returns a new http.Handler that does some work before (or instead of)
// calling the original.
func RateLimit(cfg Config) func(http.Handler) http.Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	stats := cfg.Stats
	if stats == nil {
		stats = observability.NewStats()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, err := cfg.Identify(r)
			if err != nil {
				httpjson.Error(w, http.StatusBadRequest, "invalid_client_identifier", err.Error())
				return
			}

			d, err := cfg.Limiter.Allow(r.Context(), key)
			if err != nil {
				stats.RecordError()
				// A full key table is not an outage: failing open here would
				// let anyone bypass limits by flooding new keys.
				if cfg.FailOpen && !errors.Is(err, limiter.ErrTooManyKeys) {
					stats.RecordFailOpen()
					logger.Warn("rate limiter failed; allowing request (fail-open)", "error", err)
					next.ServeHTTP(w, r)
					return
				}
				logger.Error("rate limiter failed; rejecting request", "error", err)
				w.Header().Set("Retry-After", "1")
				httpjson.Error(w, http.StatusServiceUnavailable, "rate_limiter_unavailable",
					"rate limiting is temporarily unavailable, please retry")
				return
			}

			h := w.Header()
			h.Set("RateLimit-Limit", strconv.Itoa(d.Limit))
			h.Set("RateLimit-Remaining", strconv.Itoa(d.Remaining))
			h.Set("RateLimit-Reset", strconv.Itoa(ceilSeconds(d.ResetAfter)))

			if !d.Allowed {
				stats.RecordDenied()
				retry := max(1, ceilSeconds(d.RetryAfter))
				h.Set("Retry-After", strconv.Itoa(retry))
				httpjson.Write(w, http.StatusTooManyRequests, httpjson.ErrorBody{
					Error:             "rate_limit_exceeded",
					Message:           "too many requests, please retry later",
					RetryAfterSeconds: retry,
				})
				return
			}
			stats.RecordAllowed()
			next.ServeHTTP(w, r)
		})
	}
}

// ceilSeconds rounds up, because telling a client to retry too early just
// earns it another 429.
func ceilSeconds(d time.Duration) int {
	return int(math.Ceil(d.Seconds()))
}
