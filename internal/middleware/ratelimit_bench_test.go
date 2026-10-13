package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/popeyethecode/throttlex/limiter"
)

// BenchmarkMiddleware measures the full per-request overhead of the HTTP
// layer (identify + Allow + headers) without network I/O.
func BenchmarkMiddleware(b *testing.B) {
	l, _ := limiter.NewTokenBucket(1_000_000_000, 1_000_000, limiter.Options{})
	h := RateLimit(Config{Limiter: l, Identify: identifyByHeader, Logger: quietLogger})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest(http.MethodGet, "/api/demo", nil)
		req.Header.Set("X-Client", "bench")
		for pb.Next() {
			h.ServeHTTP(httptest.NewRecorder(), req)
		}
	})
}
