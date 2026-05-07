package pipeline

import (
	"context"
	"fmt"

	"github.com/yourorg/logpipe/internal/sink"
)

// TruncateConfig holds configuration for the truncating sink wrapper.
type TruncateConfig struct {
	// MaxBytes is the maximum number of bytes allowed per log line.
	// Lines exceeding this length will be truncated and a suffix appended.
	MaxBytes int
	// Suffix is appended to truncated lines to indicate truncation occurred.
	// Defaults to "...[truncated]" if empty.
	Suffix string
}

// DefaultTruncateConfig returns a TruncateConfig with sensible defaults.
func DefaultTruncateConfig() TruncateConfig {
	return TruncateConfig{
		MaxBytes: 8192,
		Suffix:   "...[truncated]",
	}
}

// truncateSink wraps a sink.Sink and truncates lines that exceed MaxBytes.
type truncateSink struct {
	inner  sink.Sink
	config TruncateConfig
}

// NewTruncateSink returns a sink.Sink that truncates lines longer than
// config.MaxBytes before forwarding them to inner.
func NewTruncateSink(inner sink.Sink, config TruncateConfig) (sink.Sink, error) {
	if inner == nil {
		return nil, fmt.Errorf("truncate: inner sink must not be nil")
	}
	if config.MaxBytes <= 0 {
		return nil, fmt.Errorf("truncate: MaxBytes must be positive, got %d", config.MaxBytes)
	}
	if config.Suffix == "" {
		config.Suffix = DefaultTruncateConfig().Suffix
	}
	return &truncateSink{inner: inner, config: config}, nil
}

func (t *truncateSink) Name() string {
	return fmt.Sprintf("truncate(%s)", t.inner.Name())
}

func (t *truncateSink) Write(ctx context.Context, line string) error {
	if len(line) > t.config.MaxBytes {
		suffix := t.config.Suffix
		cutAt := t.config.MaxBytes - len(suffix)
		if cutAt < 0 {
			cutAt = 0
		}
		line = line[:cutAt] + suffix
	}
	return t.inner.Write(ctx, line)
}

func (t *truncateSink) Close() error {
	return t.inner.Close()
}
