package identity

import (
	"errors"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestIdentify(t *testing.T) {
	proxy := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	all := []Strategy{StrategyAPIKey, StrategyUser, StrategyIP}

	tests := []struct {
		name    string
		order   []Strategy
		trusted []netip.Prefix
		remote  string
		headers map[string]string
		want    string // exact key, or prefix when ending in "*"
		wantErr bool
	}{
		{
			name: "falls back to the peer IP", order: all,
			remote: "203.0.113.7:5555", want: "ip:203.0.113.7",
		},
		{
			name: "API key is hashed, never stored raw", order: all,
			remote: "203.0.113.7:5555", headers: map[string]string{"X-API-Key": "secret-key-123"},
			want: "apikey:*",
		},
		{
			name: "API key with control characters is rejected", order: all,
			remote: "203.0.113.7:5555", headers: map[string]string{"X-API-Key": "bad\tkey"},
			wantErr: true,
		},
		{
			name: "oversized API key is rejected", order: all,
			remote: "203.0.113.7:5555", headers: map[string]string{"X-API-Key": strings.Repeat("a", 257)},
			wantErr: true,
		},
		{
			name: "user header ignored from untrusted peer", order: all,
			remote: "203.0.113.7:5555", headers: map[string]string{"X-User-ID": "alice"},
			want: "ip:203.0.113.7",
		},
		{
			name: "user header honored from trusted proxy", order: all, trusted: proxy,
			remote: "10.1.2.3:5555", headers: map[string]string{"X-User-ID": "alice"},
			want: "user:alice",
		},
		{
			name: "malformed user ID from trusted proxy is rejected", order: all, trusted: proxy,
			remote: "10.1.2.3:5555", headers: map[string]string{"X-User-ID": "alice; drop"},
			wantErr: true,
		},
		{
			name: "X-Forwarded-For ignored from untrusted peer", order: all,
			remote: "203.0.113.7:5555", headers: map[string]string{"X-Forwarded-For": "1.2.3.4"},
			want: "ip:203.0.113.7",
		},
		{
			name: "X-Forwarded-For used from trusted proxy", order: all, trusted: proxy,
			remote: "10.1.2.3:5555", headers: map[string]string{"X-Forwarded-For": "198.51.100.9"},
			want: "ip:198.51.100.9",
		},
		{
			name: "forged leftmost X-Forwarded-For entry is skipped", order: all, trusted: proxy,
			remote: "10.1.2.3:5555", headers: map[string]string{"X-Forwarded-For": "6.6.6.6, 198.51.100.9, 10.9.9.9"},
			want: "ip:198.51.100.9",
		},
		{
			name: "IPv6 clients are grouped by /64", order: all,
			remote: "[2001:db8:1:2:aaaa:bbbb:cccc:dddd]:5555",
			want:   "ip:2001:db8:1:2::/64",
		},
		{
			name: "IPv4-mapped IPv6 is treated as IPv4", order: all,
			remote: "[::ffff:203.0.113.7]:5555", want: "ip:203.0.113.7",
		},
		{
			name: "only apikey strategy and no key is an error", order: []Strategy{StrategyAPIKey},
			remote: "203.0.113.7:5555", wantErr: true,
		},
		{
			name: "unparseable remote address is an error", order: all,
			remote: "not-an-address", wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Resolver{Order: tc.order, APIKeyHeader: "X-API-Key", UserHeader: "X-User-ID", TrustedProxies: tc.trusted}
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tc.remote
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}

			got, err := r.Identify(req)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidIdentifier) {
					t.Fatalf("got (%q, %v), want ErrInvalidIdentifier", got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if prefix, ok := strings.CutSuffix(tc.want, "*"); ok {
				if !strings.HasPrefix(got, prefix) || strings.Contains(got, "secret") {
					t.Fatalf("got %q, want prefix %q without the raw key", got, prefix)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseStrategies(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"apikey,user,ip", 3, false},
		{" IP ", 1, false},
		{"apikey,cookie", 0, true},
		{"ip,ip", 0, true},
		{"", 0, true},
	}
	for _, tc := range tests {
		got, err := ParseStrategies(tc.in)
		if (err != nil) != tc.wantErr || len(got) != tc.want {
			t.Errorf("ParseStrategies(%q) = %v, %v", tc.in, got, err)
		}
	}
}
