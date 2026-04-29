package pipeline

import (
	"context"
	"sync"
	"time"
)

// RateLimitConfig holds configuration for the rate limiter.
type RateLimitConfig struct {
	// MaxPerSecond is the maximum number of log lines allowed per second.
	MaxPerSecond int
}

// DefaultRateLimitConfig returns a sensible default rate limit config.
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		MaxPerSecond: 1000,
	}
}

// RateLimiter limits the throughput of log lines using a token bucket approach.
type RateLimiter struct {
	cfg    RateLimitConfig
	mu     sync.Mutex
	tokens int
	lastRefill time.Time
	clock  func() time.Time
}

// NewRateLimiter creates a new RateLimiter with the given config.
func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	if cfg.MaxPerSecond <= 0 {
		cfg.MaxPerSecond = DefaultRateLimitConfig().MaxPerSecond
	}
	return &RateLimiter{
		cfg:        cfg,
		tokens:     cfg.MaxPerSecond,
		lastRefill: time.Now(),
		clock:      time.Now,
	}
}

// Allow reports whether a log line should be allowed through.
// It refills tokens based on elapsed time and consumes one token per call.
func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.clock()
	elapsed := now.Sub(r.lastRefill)
	if elapsed >= time.Second {
		refill := int(elapsed.Seconds()) * r.cfg.MaxPerSecond
		r.tokens += refill
		if r.tokens > r.cfg.MaxPerSecond {
			r.tokens = r.cfg.MaxPerSecond
		}
		r.lastRefill = now
	}

	if r.tokens <= 0 {
		return false
	}
	r.tokens--
	return true
}

// Filter wraps a channel, forwarding only lines that pass the rate limit.
// Dropped lines are silently discarded. The returned channel is closed when
// in is closed or ctx is cancelled.
func (r *RateLimiter) Filter(ctx context.Context, in <-chan string) <-chan string {
	out := make(chan string, cap(in))
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-in:
				if !ok {
					return
				}
				if r.Allow() {
					select {
					case out <- line:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}
