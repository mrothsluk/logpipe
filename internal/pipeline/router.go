package pipeline

import (
	"fmt"
	"log"

	"github.com/yourorg/logpipe/internal/sink"
)

// Router fans out a single log line to multiple sinks.
// If a sink returns an error the line is still delivered to the remaining
// sinks; all errors are collected and returned.
type Router struct {
	sinks []sink.Sink
}

// NewRouter creates a Router that writes to all provided sinks.
func NewRouter(sinks []sink.Sink) *Router {
	return &Router{sinks: sinks}
}

// Route sends line to every registered sink.
// Returns a combined error when one or more sinks fail.
func (r *Router) Route(line string) error {
	var errs []error
	for _, s := range r.sinks {
		if err := s.Write(line); err != nil {
			log.Printf("[router] sink %q error: %v", s.Name(), err)
			errs = append(errs, fmt.Errorf("sink %q: %w", s.Name(), err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("route errors: %v", errs)
}

// Close shuts down all sinks in registration order.
// All errors are logged; the first non-nil error is returned.
func (r *Router) Close() error {
	var first error
	for _, s := range r.sinks {
		if err := s.Close(); err != nil {
			log.Printf("[router] close sink %q: %v", s.Name(), err)
			if first == nil {
				first = err
			}
		}
	}
	return first
}

// Sinks returns the list of registered sinks (read-only view).
func (r *Router) Sinks() []sink.Sink { return r.sinks }
