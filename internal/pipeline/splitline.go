package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/yourorg/logpipe/internal/sink"
)

// SplitLineConfig controls how lines are split before forwarding.
type SplitLineConfig struct {
	// Delimiter is the string on which each incoming line is split.
	// Defaults to "," if empty.
	Delimiter string

	// TrimSpace strips leading/trailing whitespace from each segment.
	TrimSpace bool

	// SkipEmpty discards empty segments produced after splitting.
	SkipEmpty bool
}

// DefaultSplitLineConfig returns a SplitLineConfig with sensible defaults.
func DefaultSplitLineConfig() SplitLineConfig {
	return SplitLineConfig{
		Delimiter: ",",
		TrimSpace: true,
		SkipEmpty: true,
	}
}

type splitLineSink struct {
	cfg   SplitLineConfig
	inner sink.Sink
}

// NewSplitLineSink returns a Sink that splits each incoming line on
// cfg.Delimiter and forwards every resulting segment to inner individually.
func NewSplitLineSink(cfg SplitLineConfig, inner sink.Sink) (sink.Sink, error) {
	if inner == nil {
		return nil, fmt.Errorf("splitline: inner sink must not be nil")
	}
	if cfg.Delimiter == "" {
		cfg.Delimiter = DefaultSplitLineConfig().Delimiter
	}
	return &splitLineSink{cfg: cfg, inner: inner}, nil
}

func (s *splitLineSink) Name() string {
	return "splitline(" + s.inner.Name() + ")"
}

func (s *splitLineSink) Write(ctx context.Context, line string) error {
	parts := strings.Split(line, s.cfg.Delimiter)
	for _, p := range parts {
		if s.cfg.TrimSpace {
			p = strings.TrimSpace(p)
		}
		if s.cfg.SkipEmpty && p == "" {
			continue
		}
		if err := s.inner.Write(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

func (s *splitLineSink) Close() error {
	return s.inner.Close()
}
