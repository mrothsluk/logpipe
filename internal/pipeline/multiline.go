package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/yourorg/logpipe/internal/sink"
)

// MultilineConfig holds configuration for the multiline aggregator.
type MultilineConfig struct {
	// StartPattern is a prefix that marks the beginning of a new log event.
	// Lines not matching this pattern are folded into the previous event.
	StartPattern string
	// FlushTimeout is the maximum time to wait before flushing a partial event.
	FlushTimeout time.Duration
	// MaxLines is the maximum number of lines to aggregate before forcing a flush.
	MaxLines int
}

// DefaultMultilineConfig returns a sensible default configuration.
func DefaultMultilineConfig() MultilineConfig {
	return MultilineConfig{
		StartPattern: "",
		FlushTimeout: 2 * time.Second,
		MaxLines:     100,
	}
}

// MultilineAggregator folds continuation lines into a single log event before
// forwarding to the next sink.
type MultilineAggregator struct {
	cfg   MultilineConfig
	inner sink.Sink
}

// NewMultilineAggregator creates a new MultilineAggregator wrapping inner.
// Lines that do NOT start with cfg.StartPattern are appended to the previous
// line with a newline separator. When StartPattern is empty every line is
// treated as the start of a new event (passthrough behaviour).
func NewMultilineAggregator(cfg MultilineConfig, inner sink.Sink) (*MultilineAggregator, error) {
	if inner == nil {
		return nil, errorf("multiline: inner sink must not be nil")
	}
	if cfg.MaxLines <= 0 {
		cfg.MaxLines = DefaultMultilineConfig().MaxLines
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = DefaultMultilineConfig().FlushTimeout
	}
	return &MultilineAggregator{cfg: cfg, inner: inner}, nil
}

// Name satisfies sink.Sink.
func (m *MultilineAggregator) Name() string { return "multiline(" + m.inner.Name() + ")" }

// Filter aggregates lines from ch and writes complete events to the inner sink.
func (m *MultilineAggregator) Filter(ctx context.Context, in <-chan string, out chan<- string) {
	var buf []string
	timer := time.NewTimer(m.cfg.FlushTimeout)
	defer timer.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		select {
		case out <- strings.Join(buf, "\n"):
		case <-ctx.Done():
		}
		buf = buf[:0]
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(m.cfg.FlushTimeout)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-timer.C:
			flush()
			timer.Reset(m.cfg.FlushTimeout)
		case line, ok := <-in:
			if !ok {
				flush()
				return
			}
			isStart := m.cfg.StartPattern == "" || strings.HasPrefix(line, m.cfg.StartPattern)
			if isStart && len(buf) > 0 {
				flush()
			}
			buf = append(buf, line)
			if len(buf) >= m.cfg.MaxLines {
				flush()
			}
		}
	}
}

// Close satisfies sink.Sink.
func (m *MultilineAggregator) Close() error { return m.inner.Close() }
