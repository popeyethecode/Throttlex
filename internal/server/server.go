// Package server defines ThrottleX's HTTP routes.
package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/popeyethecode/throttlex/internal/httpjson"
	"github.com/popeyethecode/throttlex/internal/observability"
)

// MaxBodyBytes caps request bodies. None of our endpoints need a body, but
// a cap stops a client from streaming gigabytes at us.
const MaxBodyBytes = 1 << 20 // 1 MiB

// Policy describes the active rate-limit policy for status/admin output.
type Policy struct {
	Backend         string   `json:"backend"`
	Algorithm       string   `json:"algorithm"`
	Limit           int      `json:"limit,omitempty"`
	Window          string   `json:"window,omitempty"`
	BucketCapacity  int      `json:"bucket_capacity,omitempty"`
	RefillPerSecond float64  `json:"refill_per_second,omitempty"`
	Identity        []string `json:"identity_order"`
	FailurePolicy   string   `json:"failure_policy"`
	MaxKeys         int      `json:"max_keys,omitempty"`
	TrustedProxies  int      `json:"trusted_proxy_ranges"`
}

// Deps are the things the routes need from the rest of the program.
type Deps struct {
	// RateLimit wraps every /api/ route.
	RateLimit func(http.Handler) http.Handler
	Stats     *observability.Stats
	Policy    Policy
	// TrackedKeys reports in-memory keys; nil when the backend is Redis.
	TrackedKeys func() int
	// AdminToken enables /api/admin/* when non-empty.
	AdminToken string
}

// NewHandler returns the root HTTP handler with every route registered.
func NewHandler(deps Deps) http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("GET /api/demo", handleDemo)
	api.HandleFunc("GET /api/status", deps.handleStatus)
	if deps.AdminToken != "" {
		api.Handle("GET /api/admin/limits", requireBearer(deps.AdminToken, http.HandlerFunc(deps.handleAdminLimits)))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth) // never rate-limited, so orchestrators can always probe it
	mux.Handle("/api/", deps.RateLimit(api))
	return limitBody(mux)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpjson.Write(w, http.StatusOK, map[string]string{"status": "ok"})
}

func handleDemo(w http.ResponseWriter, _ *http.Request) {
	httpjson.Write(w, http.StatusOK, map[string]string{
		"message": "Hello from ThrottleX! This endpoint is rate limited.",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

type statusResponse struct {
	Service     string                 `json:"service"`
	Backend     string                 `json:"backend"`
	Algorithm   string                 `json:"algorithm"`
	Stats       observability.Snapshot `json:"stats"`
	TrackedKeys *int                   `json:"tracked_keys,omitempty"`
}

func (d Deps) handleStatus(w http.ResponseWriter, _ *http.Request) {
	resp := statusResponse{
		Service:   "throttlex",
		Backend:   d.Policy.Backend,
		Algorithm: d.Policy.Algorithm,
		Stats:     d.Stats.Snapshot(),
	}
	if d.TrackedKeys != nil {
		n := d.TrackedKeys()
		resp.TrackedKeys = &n
	}
	httpjson.Write(w, http.StatusOK, resp)
}

func (d Deps) handleAdminLimits(w http.ResponseWriter, _ *http.Request) {
	httpjson.Write(w, http.StatusOK, map[string]any{"policies": []Policy{d.Policy}})
}

// requireBearer allows only requests with "Authorization: Bearer <token>".
func requireBearer(token string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		gotSum := sha256.Sum256([]byte(got))
		// Compare hashes in constant time, so response timing reveals
		// nothing about how much of the token was right.
		if !ok || subtle.ConstantTimeCompare(gotSum[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="throttlex-admin"`)
			httpjson.Error(w, http.StatusUnauthorized, "unauthorized", "valid admin bearer token required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// limitBody rejects oversized bodies up front and caps the rest.
func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > MaxBodyBytes {
			httpjson.Error(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds 1 MiB")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		next.ServeHTTP(w, r)
	})
}
