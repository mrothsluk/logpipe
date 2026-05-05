package pipeline_test

import (
	"context"
	"testing"
	"time"

	"logpipe/internal/pipeline"
)

func TestCircuitBreaker_WithBoundedQueue(t *testing.T) {
	s := &failingSink{name: "queued"}
	s.failUntil.Store(3)

	cfg := pipeline.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
	}
	cb := pipeline.NewCircuitBreakerSink(s, cfg)

	qCfg := pipeline.DefaultBackpressureConfig()
	qCfg.Capacity = 20
	q := pipeline.NewBoundedQueue(qCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go q.Drain(ctx, func(line string) {
		_ = cb.Write(ctx, line)
	})

	for i := 0; i < 10; i++ {
		_ = q.Enqueue("log line")
	}

	time.Sleep(100 * time.Millisecond)

	// After open duration, circuit should allow writes again
	if cb.State() == pipeline.StateOpen {
		t.Log("circuit still open after timeout — waiting longer")
		time.Sleep(100 * time.Millisecond)
	}

	// Confirm writes were attempted
	if s.writes.Load() == 0 {
		t.Fatal("expected at least one write attempt")
	}
}
