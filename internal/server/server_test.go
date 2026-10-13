package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/popeyethecode/throttlex/internal/observability"
)

const adminToken = "test-admin-token-0123456789"

// passThrough stands in for the rate-limit middleware, which has its own tests.
func passThrough(next http.Handler) http.Handler { return next }

func newTestHandler(admin string) http.Handler {
	return NewHandler(Deps{
		RateLimit:   passThrough,
		Stats:       observability.NewStats(),
		Policy:      Policy{Backend: "memory", Algorithm: "token_bucket", Identity: []string{"ip"}, FailurePolicy: "open"},
		TrackedKeys: func() int { return 42 },
		AdminToken:  admin,
	})
}

func TestRoutes(t *testing.T) {
	tests := []struct {
		name       string
		admin      string
		method     string
		path       string
		header     map[string]string
		body       string
		wantStatus int
	}{
		{name: "health", method: "GET", path: "/health", wantStatus: http.StatusOK},
		{name: "health wrong method", method: "POST", path: "/health", wantStatus: http.StatusMethodNotAllowed},
		{name: "demo", method: "GET", path: "/api/demo", wantStatus: http.StatusOK},
		{name: "demo wrong method", method: "DELETE", path: "/api/demo", wantStatus: http.StatusMethodNotAllowed},
		{name: "status", method: "GET", path: "/api/status", wantStatus: http.StatusOK},
		{name: "unknown path", method: "GET", path: "/nope", wantStatus: http.StatusNotFound},
		{name: "unknown api path", method: "GET", path: "/api/nope", wantStatus: http.StatusNotFound},
		{name: "admin disabled without token", method: "GET", path: "/api/admin/limits", wantStatus: http.StatusNotFound},
		{name: "admin without credentials", admin: adminToken, method: "GET", path: "/api/admin/limits", wantStatus: http.StatusUnauthorized},
		{
			name: "admin with wrong token", admin: adminToken, method: "GET", path: "/api/admin/limits",
			header: map[string]string{"Authorization": "Bearer wrong"}, wantStatus: http.StatusUnauthorized,
		},
		{
			name: "admin with correct token", admin: adminToken, method: "GET", path: "/api/admin/limits",
			header: map[string]string{"Authorization": "Bearer " + adminToken}, wantStatus: http.StatusOK,
		},
		{
			name: "oversized body is rejected", method: "GET", path: "/api/demo",
			body: strings.Repeat("x", MaxBodyBytes+1), wantStatus: http.StatusRequestEntityTooLarge,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			newTestHandler(tc.admin).ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body)
			}
		})
	}
}

func TestHealthOverRealServer(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(""))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body["status"] != "ok" {
		t.Fatalf("body = %v, err = %v", body, err)
	}
}

func TestStatusBody(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestHandler("").ServeHTTP(rec, httptest.NewRequest("GET", "/api/status", nil))

	var body statusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Algorithm != "token_bucket" || body.TrackedKeys == nil || *body.TrackedKeys != 42 {
		t.Fatalf("unexpected status body: %s", rec.Body)
	}
}

func TestAdminResponseNeverLeaksToken(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/admin/limits", nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec := httptest.NewRecorder()
	newTestHandler(adminToken).ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), adminToken) {
		t.Fatal("admin response contains the admin token")
	}
}
