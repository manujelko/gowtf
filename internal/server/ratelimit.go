package server

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateLimiter implements a simple sliding window rate limiter
type RateLimiter struct {
	requestsPerMinute int
	windowSize        time.Duration
	requests          map[string][]time.Time // key -> timestamps of requests
	mu                sync.RWMutex
	logger            *slog.Logger
}

// NewRateLimiter creates a new rate limiter with the specified requests per minute
func NewRateLimiter(requestsPerMinute int, logger *slog.Logger) *RateLimiter {
	if requestsPerMinute <= 0 {
		requestsPerMinute = 60 // Default to 60 requests per minute
	}

	rl := &RateLimiter{
		requestsPerMinute: requestsPerMinute,
		windowSize:        time.Minute,
		requests:          make(map[string][]time.Time),
		logger:            logger,
	}

	// Start background goroutine to clean up old entries
	go rl.cleanup()

	return rl
}

// cleanup periodically removes old request timestamps to prevent memory leaks
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		cutoff := time.Now().Add(-rl.windowSize)
		for key, timestamps := range rl.requests {
			// Remove old timestamps
			var valid []time.Time
			for _, ts := range timestamps {
				if ts.After(cutoff) {
					valid = append(valid, ts)
				}
			}
			if len(valid) == 0 {
				delete(rl.requests, key)
			} else {
				rl.requests[key] = valid
			}
		}
		rl.mu.Unlock()
	}
}

// Allow checks if a request from the given key should be allowed
// Returns true if allowed, false if rate limit exceeded
func (rl *RateLimiter) Allow(key string) bool {
	now := time.Now()
	cutoff := now.Add(-rl.windowSize)

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Get existing timestamps for this key
	timestamps := rl.requests[key]

	// Remove timestamps outside the window
	var valid []time.Time
	for _, ts := range timestamps {
		if ts.After(cutoff) {
			valid = append(valid, ts)
		}
	}

	// Check if we're at the limit
	if len(valid) >= rl.requestsPerMinute {
		return false
	}

	// Add current request timestamp
	valid = append(valid, now)
	rl.requests[key] = valid

	return true
}

// Remaining returns the number of remaining requests allowed for the given key in the current window
func (rl *RateLimiter) Remaining(key string) int {
	cutoff := time.Now().Add(-rl.windowSize)

	rl.mu.RLock()
	defer rl.mu.RUnlock()

	timestamps := rl.requests[key]
	var valid []time.Time
	for _, ts := range timestamps {
		if ts.After(cutoff) {
			valid = append(valid, ts)
		}
	}

	remaining := rl.requestsPerMinute - len(valid)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// getRateLimitKey extracts the key to use for rate limiting
// Uses API key if available, otherwise uses IP address
func (s *Server) getRateLimitKey(r *http.Request) string {
	// If API key authentication is enabled and request has an API key, use it
	if s.APIKey != "" {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.URL.Query().Get("api_key")
		}
		if apiKey != "" {
			// Use API key as the rate limit key (masked for logging)
			return "api_key:" + apiKey
		}
	}

	// Otherwise, use IP address
	// Try to get real IP from X-Forwarded-For or X-Real-IP headers
	ip := r.Header.Get("X-Forwarded-For")
	if ip != "" {
		// X-Forwarded-For can contain multiple IPs, take the first one
		if idx := strings.Index(ip, ","); idx != -1 {
			ip = strings.TrimSpace(ip[:idx])
		}
	}
	if ip == "" {
		ip = r.Header.Get("X-Real-IP")
	}
	if ip == "" {
		// Remove port from RemoteAddr if present
		addr := r.RemoteAddr
		if idx := strings.LastIndex(addr, ":"); idx != -1 {
			addr = addr[:idx]
		}
		ip = addr
	}

	return "ip:" + ip
}

// rateLimitMiddleware applies rate limiting to API endpoints
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	if s.rateLimiter == nil {
		// Rate limiter not configured, skip rate limiting
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only rate limit API endpoints
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}

		// Skip rate limiting for health checks (they're not under /api/)
		// This is handled by the path check above

		key := s.getRateLimitKey(r)
		if !s.rateLimiter.Allow(key) {
			remaining := s.rateLimiter.Remaining(key)
			s.loggerWithRequestID(r.Context()).Warn("Rate limit exceeded",
				"method", r.Method,
				"path", r.URL.Path,
				"key", s.maskKey(key),
				"limit", s.rateLimiter.requestsPerMinute,
				"remaining", remaining)

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(s.rateLimiter.requestsPerMinute))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
			w.Header().Set("Retry-After", "60")
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}

		// Add rate limit headers to successful requests
		remaining := s.rateLimiter.Remaining(key)
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(s.rateLimiter.requestsPerMinute))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))

		next.ServeHTTP(w, r)
	})
}

// maskKey masks the API key for logging (shows only last 4 characters)
func (s *Server) maskKey(key string) string {
	if strings.HasPrefix(key, "api_key:") {
		apiKey := key[8:] // Remove "api_key:" prefix
		if len(apiKey) <= 4 {
			return "api_key:****"
		}
		return "api_key:" + apiKey[len(apiKey)-4:] + "..."
	}
	return key
}
