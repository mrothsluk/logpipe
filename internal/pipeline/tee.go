package pipeline

import (
	"context"
	"fmt"

	"github.com/yourorg/logpipe/internal/sink"
)

// TeeSink writes each log line to a primary sink and one or more secondary
// sinks. Errors from secondary sinks are ignored so that the primary path is
// never blocked. Errors from the primary sink are returned to the caller.
type TeeSink struct {
	name      string
	primary   sink.Sink
	secondary []sink.Sink
}

// NewTeeSink creates a TeeSink that forwards every line to primary and to each
// sink in secondary. secondary may be empty, in which case TeeSink behaves
// identically to primary.
func NewTeeSink(primary sink.Sink, secondary ...sink.Sink) (*TeeSink, error) {
	if primary == nil {
		return nil, fmt.Errorf("tee: primary sink must not be nil")
	}
	return &TeeSink{
		name:      fmt.Sprintf("tee(%s)", primary.Name()),
		primary:   primary,
		secondary: secondary,
	}, nil
}

// Name returns a human-readable identifier for this sink.
func (t *TeeSink) Name() string { return t.name }

// Write sends line to the primary sink and, best-effort, to every secondary
// sink. The context is forwarded to all sinks.
func (t *TeeSink) Write(ctx context.Context, line string) error {
	// Best-effort secondary writes — never block the primary path.
	for _, s := range t.secondary {
		_ = s.Write(ctx, line)
	}
	return t.primary.Write(ctx, line)
}

// Close closes the primary sink and then all secondary sinks. All errors are
// collected; the first non-nil error is returned.
func (t *TeeSink) Close() error {
	var firstErr error
	if err := t.primary.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, s := range t.secondary {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
