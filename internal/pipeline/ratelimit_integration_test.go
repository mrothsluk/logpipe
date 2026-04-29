package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/logpipe/internal/pipeline"
)

// TestRateLimiter_WithBoundedQueue verifies that the rate limiter integrates
// correctly with a BoundedQueue: lines are enqueued, drained through a channel,
// and filtered by the rate limiter before reaching a sink.
func TestRateLimiter_WithBoundedQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cfg := pipeline.DefaultBackpressureConfig()
	cfg.Capacity = 10
	q := pipeline.NewBoundedQueue(cfg)

	lines := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, l := range lines {
		q.Enqueue(l)
	}
	q.Close()

	rlCfg := pipeline.RateLimitConfig{MaxPerSecond: 3}
	rl := pipeline.NewRateLimiter(rlCfg)

	filtered := rl.Filter(ctx, q.Drain(ctx))

	var received []string
	for line := range filtered {
		received = append(received, line)
	}

	if len(received) > 3 {
		t.Fatalf("rate limiter should have capped at 3, got %d", len(received))
	}
	if len(received) == 0 {
		t.Fatal("expected at least one line to pass through")
	}
}
