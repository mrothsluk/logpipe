package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/yourorg/logpipe/internal/sink"
)

// HeaderConfig controls how a static header line is prepended to each
// "session" (i.e. emitted once when the sink is first written to after
// construction or after a reset).
type HeaderConfig struct {
	// Header is the literal string to emit before the first log line.
	Header string
	// RepeatEvery, if > 0, re-emits the header after this many lines.
	RepeatEvery int
}

// DefaultHeaderConfig returns a HeaderConfig with sensible defaults.
func DefaultHeaderConfig() HeaderConfig {
	return HeaderConfig{
		RepeatEvery: 0,
	}
}

// HeaderSink wraps an inner Sink and injects a configurable header line
// before the first log entry (and optionally every N lines thereafter).
type HeaderSink struct {
	inner  sink.Sink
	cfg    HeaderConfig
	count  int
	emitted bool
}

// NewHeaderSink creates a HeaderSink. Returns an error when inner is nil
// or the header string is empty.
func NewHeaderSink(inner sink.Sink, cfg HeaderConfig) (*HeaderSink, error) {
	if inner == nil {
		return nil, fmt.Errorf("header: inner sink must not be nil")
	}
	if strings.TrimSpace(cfg.Header) == "" {
		return nil, fmt.Errorf("header: header string must not be empty")
	}
	return &HeaderSink{inner: inner, cfg: cfg}, nil
}

// Name returns a descriptive name for the sink.
func (h *HeaderSink) Name() string {
	return fmt.Sprintf("header(%s)", h.inner.Name())
}

// Write emits the header when required, then forwards line to the inner sink.
func (h *HeaderSink) Write(ctx context.Context, line string) error {
	needHeader := !h.emitted ||
		(h.cfg.RepeatEvery > 0 && h.count > 0 && h.count%h.cfg.RepeatEvery == 0)

	if needHeader {
		if err := h.inner.Write(ctx, h.cfg.Header); err != nil {
			return fmt.Errorf("header: writing header: %w", err)
		}
		h.emitted = true
	}

	h.count++
	return h.inner.Write(ctx, line)
}

// Close closes the underlying sink.
func (h *HeaderSink) Close() error {
	return h.inner.Close()
}
