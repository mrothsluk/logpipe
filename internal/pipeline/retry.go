package pipeline

import (
	"context"
	"time"

	"github.com/yourorg/logpipe/internal/sink"
)

// RetryConfig holds configuration for the retry middleware.
type RetryConfig struct {
	MaxAttempts int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// DefaultRetryConfig returns a RetryConfig with sensible defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		Multiplier:   2.0,
	}
}

// RetrySink wraps a sink.Sink and retries failed writes with exponential backoff.
type RetrySink struct {
	inner  sink.Sink
	cfg    RetryConfig
	sleepFn func(d time.Duration)
}

// NewRetrySink creates a RetrySink wrapping the provided sink.
func NewRetrySink(inner sink.Sink, cfg RetryConfig) *RetrySink {
	return &RetrySink{
		inner:   inner,
		cfg:     cfg,
		sleepFn: time.Sleep,
	}
}

// Name returns the underlying sink name with a retry prefix.
func (r *RetrySink) Name() string {
	return "retry(" + r.inner.Name() + ")"
}

// Write attempts to write line to the inner sink, retrying on failure.
func (r *RetrySink) Write(ctx context.Context, line string) error {
	delay := r.cfg.InitialDelay
	var lastErr error
	for attempt := 0; attempt < r.cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.inner.Write(ctx, line); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < r.cfg.MaxAttempts-1 {
			r.sleepFn(delay)
			delay = time.Duration(float64(delay) * r.cfg.Multiplier)
			if delay > r.cfg.MaxDelay {
				delay = r.cfg.MaxDelay
			}
		}
	}
	return lastErr
}

// Close closes the underlying sink.
func (r *RetrySink) Close() error {
	return r.inner.Close()
}
