package pipeline

import (
	"context"
	"fmt"
	"regexp"

	"github.com/yourorg/logpipe/internal/sink"
)

// MaskConfig holds configuration for the masking sink.
type MaskConfig struct {
	// Patterns maps a compiled regex to its replacement string.
	Patterns map[*regexp.Regexp]string
}

// MaskSink wraps an inner sink and redacts sensitive data in log lines
// before forwarding them.
type MaskSink struct {
	inner   sink.Sink
	config  MaskConfig
}

// NewMaskSink creates a MaskSink that applies each pattern/replacement pair
// to every log line before passing it to inner.
// Returns an error if inner is nil or no patterns are provided.
func NewMaskSink(inner sink.Sink, cfg MaskConfig) (*MaskSink, error) {
	if inner == nil {
		return nil, fmt.Errorf("mask: inner sink must not be nil")
	}
	if len(cfg.Patterns) == 0 {
		return nil, fmt.Errorf("mask: at least one pattern is required")
	}
	return &MaskSink{inner: inner, config: cfg}, nil
}

// Name returns the name of the inner sink prefixed with "mask:".
func (m *MaskSink) Name() string {
	return "mask:" + m.inner.Name()
}

// Write applies all masking patterns to line and forwards the result.
func (m *MaskSink) Write(ctx context.Context, line string) error {
	masked := line
	for re, replacement := range m.config.Patterns {
		masked = re.ReplaceAllString(masked, replacement)
	}
	return m.inner.Write(ctx, masked)
}

// Close closes the inner sink.
func (m *MaskSink) Close() error {
	return m.inner.Close()
}

// MustCompilePattern is a helper that compiles a regex and pairs it with a
// replacement string, panicking on invalid expressions.
func MustCompilePattern(pattern, replacement string) (map[*regexp.Regexp]string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("mask: invalid pattern %q: %w", pattern, err)
	}
	return map[*regexp.Regexp]string{re: replacement}, nil
}
