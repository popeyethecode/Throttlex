// Package identity decides which client a request belongs to, producing the
// key the rate limiter counts against.
package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
)

// Strategy is one way of identifying a client.
type Strategy string

const (
	// StrategyAPIKey uses an API key header. The key is hashed before use,
	// so raw keys never sit in limiter memory, Redis, or logs.
	StrategyAPIKey Strategy = "apikey"
	// StrategyUser uses a user ID header set by a trusted authentication
	// proxy. It is only honored when the request comes from a trusted proxy,
	// because anyone on the internet can send any header.
	StrategyUser Strategy = "user"
	// StrategyIP uses the client IP address. It always succeeds, so it is
	// the natural last fallback.
	StrategyIP Strategy = "ip"
)

// ErrInvalidIdentifier means the request carried a malformed identifier.
var ErrInvalidIdentifier = errors.New("invalid client identifier")

const (
	maxAPIKeyLen = 256
	maxUserIDLen = 128
)

// Resolver turns a request into a rate-limit key.
type Resolver struct {
	// Order lists strategies to try, first match wins.
	Order []Strategy
	// APIKeyHeader is the header carrying the API key, e.g. "X-API-Key".
	APIKeyHeader string
	// UserHeader is the header carrying the authenticated user ID.
	UserHeader string
	// TrustedProxies lists networks whose requests may set UserHeader and
	// X-Forwarded-For. Empty means trust no proxy (the safe default).
	TrustedProxies []netip.Prefix
}

// Identify returns a key such as "apikey:3f1a...", "user:alice" or
// "ip:203.0.113.7". It returns an error wrapping ErrInvalidIdentifier if a
// supplied identifier is malformed.
func (r *Resolver) Identify(req *http.Request) (string, error) {
	peer, err := netip.ParseAddrPort(req.RemoteAddr)
	if err != nil {
		return "", fmt.Errorf("%w: unparseable remote address", ErrInvalidIdentifier)
	}
	peerIP := peer.Addr().Unmap()
	fromTrustedProxy := r.isTrusted(peerIP)

	for _, s := range r.Order {
		switch s {
		case StrategyAPIKey:
			key := req.Header.Get(r.APIKeyHeader)
			if key == "" {
				continue
			}
			if len(key) > maxAPIKeyLen || !isPrintableASCII(key) {
				return "", fmt.Errorf("%w: malformed API key", ErrInvalidIdentifier)
			}
			sum := sha256.Sum256([]byte(key))
			return "apikey:" + hex.EncodeToString(sum[:16]), nil

		case StrategyUser:
			if !fromTrustedProxy {
				continue
			}
			user := req.Header.Get(r.UserHeader)
			if user == "" {
				continue
			}
			if len(user) > maxUserIDLen || !isSafeUserID(user) {
				return "", fmt.Errorf("%w: malformed user ID", ErrInvalidIdentifier)
			}
			return "user:" + user, nil

		case StrategyIP:
			ip := peerIP
			if fromTrustedProxy {
				ip = r.forwardedClientIP(req, peerIP)
			}
			return "ip:" + ipKey(ip), nil
		}
	}
	return "", fmt.Errorf("%w: no identification strategy matched", ErrInvalidIdentifier)
}

// forwardedClientIP walks X-Forwarded-For from right to left. Each proxy
// appends the address it received the request from, so the rightmost
// entries were written by our own (trusted) proxies, and the first
// untrusted address from the right is the real client. Entries further
// left were supplied by the client and could be forged.
func (r *Resolver) forwardedClientIP(req *http.Request, peer netip.Addr) netip.Addr {
	var hops []string
	for _, h := range req.Header.Values("X-Forwarded-For") {
		hops = append(hops, strings.Split(h, ",")...)
	}
	client := peer
	for i := len(hops) - 1; i >= 0; i-- {
		ip, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			break // garbage in the header: stop and use the last good hop
		}
		client = ip.Unmap()
		if !r.isTrusted(client) {
			break
		}
	}
	return client
}

func (r *Resolver) isTrusted(ip netip.Addr) bool {
	for _, p := range r.TrustedProxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// ipKey groups IPv6 clients by /64 network. A single IPv6 customer is
// usually given a whole /64 (2^64 addresses), so per-address limits would
// be trivial to evade by rotating addresses.
func ipKey(ip netip.Addr) string {
	if ip.Is6() {
		prefix, _ := ip.Prefix(64)
		return prefix.String()
	}
	return ip.String()
}

func isPrintableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

func isSafeUserID(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' || c == '@'
		if !ok {
			return false
		}
	}
	return true
}

// ParseStrategies parses a comma-separated list such as "apikey,user,ip".
func ParseStrategies(s string) ([]Strategy, error) {
	var out []Strategy
	seen := map[Strategy]bool{}
	for _, part := range strings.Split(s, ",") {
		st := Strategy(strings.ToLower(strings.TrimSpace(part)))
		switch st {
		case StrategyAPIKey, StrategyUser, StrategyIP:
		default:
			return nil, fmt.Errorf("unknown identity strategy %q (want apikey, user, or ip)", part)
		}
		if seen[st] {
			return nil, fmt.Errorf("identity strategy %q listed twice", st)
		}
		seen[st] = true
		out = append(out, st)
	}
	return out, nil
}
