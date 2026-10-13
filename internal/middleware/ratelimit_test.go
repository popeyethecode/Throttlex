package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/popeyethecode/throttlex/internal/httpjson"
	"github.com/popeyethecode/throttlex/internal/observability"
	"github.com/popeyethecode/throttlex/limiter"
)

// stubLimiter returns a fixed decision or error, so middleware tests do not
// depend on any real algorithm.
type stubLimiter struct {
	d   limiter.Decision
	err error
}

func (s stubLimiter) Allow(context.Context, string) (limiter.Decision, error) { return s.d, s.err }

func identifyByHeader(r *http.Request) (string, error) {
	if id := r.Header.Get("X-Client"); id != "" {
		return id, nil
	}
	return "", errors.New("no client")
}

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "ok")
})

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRateLimitResponses(t *testing.T) {
	tests := []struct {
		name        string
		limiter     limiter.Limiter
		failOpen    bool
		client      string
		wantStatus  int
		wantHeaders map[string]string // "" means the header must be absent
		wantError   string            // JSON error code, if any
	}{
		{
			name:       "allowed request passes through with headers",
			limiter:    stubLimiter{d: limiter.Decision{Allowed: true, Limit: 10, Remaining: 7, ResetAfter: 1500 * time.Millisecond}},
			client:     "a",
			wantStatus: http.StatusOK,
			wantHeaders: map[string]string{
				"RateLimit-Limit": "10", "RateLimit-Remaining": "7", "RateLimit-Reset": "2", "Retry-After": "",
			},
		},
		{
			name:       "denied request gets 429 and Retry-After",
			limiter:    stubLimiter{d: limiter.Decision{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 2100 * time.Millisecond, ResetAfter: 30 * time.Second}},
			client:     "a",
			wantStatus: http.StatusTooManyRequests,
			wantHeaders: map[string]string{
				"RateLimit-Limit": "10", "RateLimit-Remaining": "0", "RateLimit-Reset": "30", "Retry-After": "3",
				"Content-Type": "application/json",
			},
			wantError: "rate_limit_exceeded",
		},
		{
			name:        "sub-second retry is rounded up to 1",
			limiter:     stubLimiter{d: limiter.Decision{Allowed: false, Limit: 1, RetryAfter: time.Millisecond}},
			client:      "a",
			wantStatus:  http.StatusTooManyRequests,
			wantHeaders: map[string]string{"Retry-After": "1"},
			wantError:   "rate_limit_exceeded",
		},
		{
			name:       "unidentifiable client gets 400",
			limiter:    stubLimiter{d: limiter.Decision{Allowed: true}},
			client:     "",
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_client_identifier",
		},
		{
			name:        "limiter failure with fail-closed gets 503",
			limiter:     stubLimiter{err: errors.New("redis: connection refused")},
			client:      "a",
			wantStatus:  http.StatusServiceUnavailable,
			wantHeaders: map[string]string{"Retry-After": "1", "RateLimit-Limit": ""},
			wantError:   "rate_limiter_unavailable",
		},
		{
			name:        "limiter failure with fail-open passes through without quota headers",
			limiter:     stubLimiter{err: errors.New("redis: i/o timeout")},
			failOpen:    true,
			client:      "a",
			wantStatus:  http.StatusOK,
			wantHeaders: map[string]string{"RateLimit-Limit": ""},
		},
		{
			name:       "key table full is rejected even with fail-open",
			limiter:    stubLimiter{err: limiter.ErrTooManyKeys},
			failOpen:   true,
			client:     "a",
			wantStatus: http.StatusServiceUnavailable,
			wantError:  "rate_limiter_unavailable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := RateLimit(Config{
				Limiter: tc.limiter, Identify: identifyByHeader, FailOpen: tc.failOpen, Logger: quietLogger,
			})(okHandler)

			req := httptest.NewRequest(http.MethodGet, "/api/demo", nil)
			if tc.client != "" {
				req.Header.Set("X-Client", tc.client)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body)
			}
			for name, want := range tc.wantHeaders {
				if got := rec.Header().Get(name); got != want {
					t.Errorf("header %s = %q, want %q", name, got, want)
				}
			}
			if tc.wantError != "" {
				var body httpjson.ErrorBody
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("error body is not JSON: %v", err)
				}
				if body.Error != tc.wantError {
					t.Errorf("error code = %q, want %q", body.Error, tc.wantError)
				}
			}
		})
	}
}

func TestStatsAreRecorded(t *testing.T) {
	stats := observability.NewStats()
	clk := limiter.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	l, _ := limiter.NewFixedWindow(2, time.Minute, limiter.Options{Clock: clk})
	h := RateLimit(Config{Limiter: l, Identify: identifyByHeader, Stats: stats, Logger: quietLogger})(okHandler)

	for range 5 {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Client", "a")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	s := stats.Snapshot()
	if s.Allowed != 2 || s.Denied != 3 {
		t.Fatalf("allowed=%d denied=%d, want 2 and 3", s.Allowed, s.Denied)
	}
}

// TestConcurrentHTTPRequests fires many simultaneous real HTTP requests
// from one client through a real limiter. Exactly `limit` may succeed.
func TestConcurrentHTTPRequests(t *testing.T) {
	const limit, total = 25, 200
	clk := limiter.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	l, _ := limiter.NewTokenBucket(limit, 0.001, limiter.Options{Clock: clk})
	srv := httptest.NewServer(RateLimit(Config{Limiter: l, Identify: identifyByHeader, Logger: quietLogger})(okHandler))
	defer srv.Close()

	var mu sync.Mutex
	codes := map[int]int{}
	var wg sync.WaitGroup
	for range total {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
			req.Header.Set("X-Client", "same-client")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			mu.Lock()
			codes[resp.StatusCode]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if codes[http.StatusOK] != limit || codes[http.StatusTooManyRequests] != total-limit {
		t.Fatalf("status counts = %v, want %d x 200 and %d x 429", codes, limit, total-limit)
	}
}
