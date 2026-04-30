package pipeline

import (
	"testing"
	"time"
)

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()

	if cfg.MaxAttempts != 3 {
		t.Errorf("expected MaxAttempts=3, got %d", cfg.MaxAttempts)
	}
	if cfg.InitialDelay != 100*time.Millisecond {
		t.Errorf("expected InitialDelay=100ms, got %v", cfg.InitialDelay)
	}
	if cfg.MaxDelay != 2*time.Second {
		t.Errorf("expected MaxDelay=2s, got %v", cfg.MaxDelay)
	}
	if cfg.Multiplier != 2.0 {
		t.Errorf("expected Multiplier=2.0, got %f", cfg.Multiplier)
	}
}

func TestRetrySink_ExponentialBackoffCaps(t *testing.T) {
	ms := &mockSink{name: "test", failUntil: 10}
	cfg := RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     200 * time.Millisecond,
		Multiplier:   10.0,
	}
	var sleepDurations []time.Duration
	rs := NewRetrySink(ms, cfg)
	rs.sleepFn = func(d time.Duration) {
		sleepDurations = append(sleepDurations, d)
	}

	// Ignore error; we just care about sleep durations.
	_ = rs.Write(contextBackground(), "line")

	for _, d := range sleepDurations {
		if d > cfg.MaxDelay {
			t.Errorf("sleep duration %v exceeded MaxDelay %v", d, cfg.MaxDelay)
		}
	}
}

func TestRetrySink_ZeroMaxAttempts_NeverWrites(t *testing.T) {
	ms := &mockSink{name: "test", failUntil: 0}
	cfg := RetryConfig{MaxAttempts: 0}
	rs := NewRetrySink(ms, cfg)
	rs.sleepFn = noSleep
	// With 0 max attempts the loop body never executes, so Write returns nil (lastErr).
	_ = rs.Write(contextBackground(), "line")
	if ms.callCount != 0 {
		t.Errorf("expected 0 calls, got %d", ms.callCount)
	}
}

// contextBackground is a helper to avoid import cycle in test files.
func contextBackground() interface{ Err() error } {
	import_ctx_bg, _ := func() (interface{ Err() error }, func()) {
		import (
			"context"
		)
		return context.Background(), func() {}
	}()
	return import_ctx_bg
}
