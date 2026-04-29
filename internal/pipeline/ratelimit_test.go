package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiter_AllowsUpToLimit(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{MaxPerSecond: 3})
	rl.clock = fixedRLClock(time.Now())

	allowed := 0
	for i := 0; i < 5; i++ {
		if rl.Allow() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("expected 3 allowed, got %d", allowed)
	}
}

func TestRateLimiter_RefillsAfterSecond(t *testing.T) {
	now := time.Now()
	rl := NewRateLimiter(RateLimitConfig{MaxPerSecond: 2})
	rl.clock = fixedRLClock(now)

	// exhaust tokens
	rl.Allow()
	rl.Allow()
	if rl.Allow() {
		t.Fatal("expected third call to be denied")
	}

	// advance clock by 1 second to trigger refill
	rl.clock = fixedRLClock(now.Add(time.Second + time.Millisecond))
	if !rl.Allow() {
		t.Fatal("expected allow after refill")
	}
}

func TestRateLimiter_DefaultConfig(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{MaxPerSecond: 0})
	if rl.cfg.MaxPerSecond != DefaultRateLimitConfig().MaxPerSecond {
		t.Fatalf("expected default max per second %d, got %d",
			DefaultRateLimitConfig().MaxPerSecond, rl.cfg.MaxPerSecond)
	}
}

func TestRateLimiter_Filter_ForwardsAllowed(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{MaxPerSecond: 2})
	now := time.Now()
	rl.clock = fixedRLClock(now)

	in := make(chan string, 5)
	in <- "line1"
	in <- "line2"
	in <- "line3" // should be dropped
	close(in)

	ctx := context.Background()
	out := rl.Filter(ctx, in)

	var received []string
	for line := range out {
		received = append(received, line)
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 lines, got %d: %v", len(received), received)
	}
}

func TestRateLimiter_Filter_ContextCancel(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{MaxPerSecond: 100})

	in := make(chan string)
	ctx, cancel := context.WithCancel(context.Background())
	out := rl.Filter(ctx, in)

	cancel()
	// out should close after cancel
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected channel to be closed after context cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

// fixedRLClock returns a clock function that always returns t.
func fixedRLClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}
