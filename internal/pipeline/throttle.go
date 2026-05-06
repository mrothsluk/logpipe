package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/logpipe/logpipe/internal/sink"
)

// ThrottleConfig controls per-key throttling behaviour.
type ThrottleConfig struct {
	// Interval is the minimum duration between forwarded lines.
	Interval time.Duration
	// KeyFunc extracts a throttle key from a line. If nil, all lines share one key.
	KeyFunc func(line string) string
}

// DefaultThrottleConfig returns a sensible default (1 line per second, global key).
func DefaultThrottleConfig() ThrottleConfig {
	return ThrottleConfig{
		Interval: time.Second,
		KeyFunc:  nil,
	}
}

type throttleSink struct {
	inner  sink.Sink
	cfg    ThrottleConfig
	mu     sync.Mutex
	lastAt map[string]time.Time
}

// NewThrottleSink wraps inner and suppresses lines that arrive within
// cfg.Interval of the previous forwarded line for the same key.
func NewThrottleSink(inner sink.Sink, cfg ThrottleConfig) (sink.Sink, error) {
	if inner == nil {
		return nil, errorf("throttle: inner sink must not be nil")
	}
	if cfg.Interval <= 0 {
		return nil, errorf("throttle: interval must be positive")
	}
	return &throttleSink{
		inner:  inner,
		cfg:    cfg,
		lastAt: make(map[string]time.Time),
	}, nil
}

func (t *throttleSink) Name() string { return "throttle(" + t.inner.Name() + ")" }

func (t *throttleSink) Write(ctx context.Context, line string) error {
	key := ""
	if t.cfg.KeyFunc != nil {
		key = t.cfg.KeyFunc(line)
	}

	now := time.Now()
	t.mu.Lock()
	last, seen := t.lastAt[key]
	if seen && now.Sub(last) < t.cfg.Interval {
		t.mu.Unlock()
		return nil // suppressed
	}
	t.lastAt[key] = now
	t.mu.Unlock()

	return t.inner.Write(ctx, line)
}

func (t *throttleSink) Close() error { return t.inner.Close() }
