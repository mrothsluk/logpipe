package pipeline_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"logpipe/internal/pipeline"
)

type failingSink struct {
	name     string
	writes   atomic.Int32
	failUntil atomic.Int32
}

func (f *failingSink) Name() string { return f.name }
func (f *failingSink) Close() error { return nil }
func (f *failingSink) Write(_ context.Context, _ string) error {
	f.writes.Add(1)
	if int(f.failUntil.Load()) >= int(f.writes.Load()) {
		return errors.New("sink error")
	}
	return nil
}

func TestCircuitBreaker_Name(t *testing.T) {
	s := &failingSink{name: "test"}
	cb := pipeline.NewCircuitBreakerSink(s, pipeline.DefaultCircuitBreakerConfig())
	if cb.Name() != "circuit:test" {
		t.Fatalf("expected 'circuit:test', got %q", cb.Name())
	}
}

func TestCircuitBreaker_ClosedByDefault(t *testing.T) {
	s := &failingSink{name: "s"}
	cb := pipeline.NewCircuitBreakerSink(s, pipeline.DefaultCircuitBreakerConfig())
	if cb.State() != pipeline.StateClosed {
		t.Fatal("expected closed state initially")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	s := &failingSink{name: "s"}
	s.failUntil.Store(10)
	cfg := pipeline.CircuitBreakerConfig{FailureThreshold: 3, SuccessThreshold: 2, OpenDuration: time.Hour}
	cb := pipeline.NewCircuitBreakerSink(s, cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = cb.Write(ctx, "line")
	}
	if cb.State() != pipeline.StateOpen {
		t.Fatal("expected circuit to be open after threshold failures")
	}
}

func TestCircuitBreaker_RejectsWhenOpen(t *testing.T) {
	s := &failingSink{name: "s"}
	s.failUntil.Store(10)
	cfg := pipeline.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, OpenDuration: time.Hour}
	cb := pipeline.NewCircuitBreakerSink(s, cfg)
	ctx := context.Background()

	_ = cb.Write(ctx, "line") // trips the breaker
	err := cb.Write(ctx, "line")
	if !errors.Is(err, pipeline.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_HalfOpenAfterDuration(t *testing.T) {
	s := &failingSink{name: "s"}
	s.failUntil.Store(1)
	cfg := pipeline.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, OpenDuration: 10 * time.Millisecond}
	cb := pipeline.NewCircuitBreakerSink(s, cfg)
	ctx := context.Background()

	_ = cb.Write(ctx, "line") // open
	time.Sleep(20 * time.Millisecond)
	// next write should attempt (half-open), succeed, close
	err := cb.Write(ctx, "line")
	if err != nil {
		t.Fatalf("expected success in half-open, got %v", err)
	}
	if cb.State() != pipeline.StateClosed {
		t.Fatalf("expected closed after recovery, got %v", cb.State())
	}
}

// TestCircuitBreaker_HalfOpenFailureReopens verifies that a failure during the
// half-open state transitions the circuit back to open rather than closed.
func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	s := &failingSink{name: "s"}
	// First write fails (opens breaker), second write also fails (half-open failure).
	s.failUntil.Store(2)
	cfg := pipeline.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, OpenDuration: 10 * time.Millisecond}
	cb := pipeline.NewCircuitBreakerSink(s, cfg)
	ctx := context.Background()

	_ = cb.Write(ctx, "line") // trips the breaker → open
	time.Sleep(20 * time.Millisecond)
	// Half-open probe fails → should reopen
	_ = cb.Write(ctx, "line")
	if cb.State() != pipeline.StateOpen {
		t.Fatalf("expected circuit to reopen after half-open failure, got %v", cb.State())
	}
}

func TestCircuitBreaker_DefaultConfig(t *testing.T) {
	cfg := pipeline.DefaultCircuitBreakerConfig()
	if cfg.FailureThreshold <= 0 {
		t.Fatal("failure threshold must be positive")
	}
	if cfg.OpenDuration <= 0 {
		t.Fatal("open duration must be positive")
	}
}
