package sink

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// StdoutSink writes log lines to an io.Writer (defaults to os.Stdout).
type StdoutSink struct {
	mu     sync.Mutex
	w      io.Writer
	prefix string
}

// NewStdoutSink creates a StdoutSink. If w is nil, os.Stdout is used.
func NewStdoutSink(w io.Writer, prefix string) *StdoutSink {
	if w == nil {
		w = os.Stdout
	}
	return &StdoutSink{w: w, prefix: prefix}
}

// Name implements Sink.
func (s *StdoutSink) Name() string { return "stdout" }

// Write implements Sink. It is safe for concurrent use.
func (s *StdoutSink) Write(_ context.Context, line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prefix != "" {
		_, err := fmt.Fprintf(s.w, "%s %s\n", s.prefix, line)
		return err
	}
	_, err := fmt.Fprintln(s.w, line)
	return err
}

// Close implements Sink. StdoutSink has nothing to flush.
func (s *StdoutSink) Close() error { return nil }
