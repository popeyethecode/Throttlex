package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

// fakeEnv turns a map into a getenv function.
func fakeEnv(m map[string]string) func(string) string {
	return func(key string) string { return m[key] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(fakeEnv(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"Addr", cfg.Addr, ":8080"},
		{"LogLevel", cfg.LogLevel, slog.LevelInfo},
		{"Backend", cfg.Backend, BackendMemory},
		{"Algorithm", cfg.Algorithm, TokenBucket},
		{"Limit", cfg.Limit, 100},
		{"Window", cfg.Window, time.Minute},
		{"BucketCapacity", cfg.BucketCapacity, 100},
		{"MaxKeys", cfg.MaxKeys, 100_000},
		{"FailOpen", cfg.FailOpen, true},
		{"Identity count", len(cfg.Identity), 3},
		{"AdminToken", cfg.AdminToken, ""},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// 100 requests per minute.
	if diff := cfg.RefillPerSecond - 100.0/60; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("RefillPerSecond = %v, want %v", cfg.RefillPerSecond, 100.0/60)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(fakeEnv(map[string]string{
		"THROTTLEX_ADDR":            "127.0.0.1:9090",
		"THROTTLEX_LOG_LEVEL":       "DEBUG",
		"THROTTLEX_BACKEND":         "redis",
		"THROTTLEX_ALGORITHM":       "fixed_window",
		"THROTTLEX_LIMIT":           "5",
		"THROTTLEX_WINDOW":          "10s",
		"THROTTLEX_IDENTITY":        "ip",
		"THROTTLEX_TRUSTED_PROXIES": "10.0.0.0/8, 192.168.1.1/32",
		"THROTTLEX_FAILURE_POLICY":  "closed",
		"THROTTLEX_ADMIN_TOKEN":     "0123456789abcdef",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Addr != "127.0.0.1:9090" || cfg.LogLevel != slog.LevelDebug || cfg.Backend != BackendRedis ||
		cfg.Algorithm != FixedWindow || cfg.Limit != 5 || cfg.Window != 10*time.Second ||
		len(cfg.Identity) != 1 || len(cfg.TrustedProxies) != 2 || cfg.FailOpen {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{"address without port", map[string]string{"THROTTLEX_ADDR": "8080"}, "THROTTLEX_ADDR"},
		{"port out of range", map[string]string{"THROTTLEX_ADDR": ":70000"}, "THROTTLEX_ADDR"},
		{"unknown log level", map[string]string{"THROTTLEX_LOG_LEVEL": "loud"}, "THROTTLEX_LOG_LEVEL"},
		{"unknown algorithm", map[string]string{"THROTTLEX_ALGORITHM": "leaky"}, "THROTTLEX_ALGORITHM"},
		{"unknown backend", map[string]string{"THROTTLEX_BACKEND": "memcached"}, "THROTTLEX_BACKEND"},
		{"redis with unsupported algorithm", map[string]string{"THROTTLEX_BACKEND": "redis", "THROTTLEX_ALGORITHM": "sliding_log"}, "supports only"},
		{"non-numeric limit", map[string]string{"THROTTLEX_LIMIT": "lots"}, "THROTTLEX_LIMIT"},
		{"zero limit", map[string]string{"THROTTLEX_LIMIT": "0"}, "THROTTLEX_LIMIT"},
		{"sliding log limit too large", map[string]string{"THROTTLEX_ALGORITHM": "sliding_log", "THROTTLEX_LIMIT": "200000"}, "sliding_log"},
		{"bad window", map[string]string{"THROTTLEX_WINDOW": "1 minute"}, "THROTTLEX_WINDOW"},
		{"negative refill", map[string]string{"THROTTLEX_REFILL_RATE": "-1"}, "THROTTLEX_REFILL_RATE"},
		{"zero max keys", map[string]string{"THROTTLEX_MAX_KEYS": "0"}, "THROTTLEX_MAX_KEYS"},
		{"cleanup too frequent", map[string]string{"THROTTLEX_CLEANUP_INTERVAL": "10ms"}, "THROTTLEX_CLEANUP_INTERVAL"},
		{"bad failure policy", map[string]string{"THROTTLEX_FAILURE_POLICY": "maybe"}, "THROTTLEX_FAILURE_POLICY"},
		{"bad identity", map[string]string{"THROTTLEX_IDENTITY": "cookie"}, "THROTTLEX_IDENTITY"},
		{"bad proxy CIDR", map[string]string{"THROTTLEX_TRUSTED_PROXIES": "10.0.0.1"}, "THROTTLEX_TRUSTED_PROXIES"},
		{"short admin token", map[string]string{"THROTTLEX_ADMIN_TOKEN": "short"}, "THROTTLEX_ADMIN_TOKEN"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(fakeEnv(tc.env))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}
}

func TestLoadReportsAllErrorsAtOnce(t *testing.T) {
	_, err := Load(fakeEnv(map[string]string{
		"THROTTLEX_LIMIT":     "0",
		"THROTTLEX_ALGORITHM": "nope",
	}))
	if err == nil || !strings.Contains(err.Error(), "THROTTLEX_LIMIT") || !strings.Contains(err.Error(), "THROTTLEX_ALGORITHM") {
		t.Fatalf("want both problems reported, got %v", err)
	}
}
