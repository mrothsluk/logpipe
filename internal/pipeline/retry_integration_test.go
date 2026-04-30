package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// flakyMockSink fails the first N writes then succeeds.
type flakyMockSink struct {
	failUntil int
	calls     int
	received  []string
}

func (f *flakyMockSink) Name() string { return "flaky" }
func (f *flakyMockSink) Close() error { return nil }
func (f *flakyMockSink) Write(_ context.Context, line string) error {
	f.calls++
	if f.calls <= f.failUntil {
		return errors.New("transient error")
	}
	f.received = append(f.received, line)
	return nil
}

// TestRetrySink_WithBoundedQueue verifies that a RetrySink works correctly
// when lines are fed through a BoundedQueue.
func TestRetrySink_WithBoundedQueue(t *testing.T) {
	flaky := &flakyMockSink{failUntil: 1}
	cfg := RetryConfig{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
	}
	rs := NewRetrySink(flaky, cfg)
	rs.sleepFn = func(d time.Duration) { time.Sleep(d) }

	q := NewBoundedQueue(DefaultBackpressureConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	lines := []string{"alpha", "beta", "gamma"}
	for _, l := range lines {
		if err := q.Enqueue(ctx, l); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}
	q.Close()

	for line := range q.Drain() {
		if err := rs.Write(ctx, line); err != nil {
			t.Errorf("unexpected write error for line %q: %v", line, err)
		}
	}

	if len(flaky.received) != len(lines) {
		t.Errorf("expected %d lines received, got %d", len(lines), len(flaky.received))
	}
}
