package pipeline

import (
	"context"
	"regexp"

	"github.com/yourorg/logpipe/internal/sink"
)

// FilterConfig holds configuration for the line filter.
type FilterConfig struct {
	// IncludePattern, if non-empty, only passes lines matching this regex.
	IncludePattern string
	// ExcludePattern, if non-empty, drops lines matching this regex.
	ExcludePattern string
}

// Filter passes or drops log lines based on regex patterns.
type Filter struct {
	cfg     FilterConfig
	include *regexp.Regexp
	exclude *regexp.Regexp
	next    sink.Sink
}

// NewFilter constructs a Filter wrapping the next sink.
// Returns an error if either pattern fails to compile.
func NewFilter(cfg FilterConfig, next sink.Sink) (*Filter, error) {
	f := &Filter{cfg: cfg, next: next}
	if cfg.IncludePattern != "" {
		re, err := regexp.Compile(cfg.IncludePattern)
		if err != nil {
			return nil, err
		}
		f.include = re
	}
	if cfg.ExcludePattern != "" {
		re, err := regexp.Compile(cfg.ExcludePattern)
		if err != nil {
			return nil, err
		}
		f.exclude = re
	}
	return f, nil
}

// Name returns the filter's identifier.
func (f *Filter) Name() string { return "filter" }

// Write evaluates the line against include/exclude patterns before
// forwarding to the wrapped sink.
func (f *Filter) Write(ctx context.Context, line string) error {
	if f.include != nil && !f.include.MatchString(line) {
		return nil
	}
	if f.exclude != nil && f.exclude.MatchString(line) {
		return nil
	}
	return f.next.Write(ctx, line)
}

// Close closes the underlying sink.
func (f *Filter) Close() error { return f.next.Close() }
