package pipeline

import (
	"context"
	"fmt"
	"log"

	"github.com/yourorg/logpipe/internal/sink"
)

// Router fans a stream of log lines out to multiple sinks.
// Each sink gets its own BoundedQueue so a slow sink cannot stall others.
type Router struct {
	sinks  []sink.Sink
	queues []*BoundedQueue
}

// NewRouter creates a Router that writes to the provided sinks.
// bpCfg controls backpressure per-sink queue.
func NewRouter(sinks []sink.Sink, bpCfg BackpressureConfig) *Router {
	r := &Router{sinks: sinks}
	for _, s := range sinks {
		s := s // capture
		q := NewBoundedQueue(bpCfg, func(line string) {
			if err := s.Write(line); err != nil {
				log.Printf("[router] sink %s write error: %v", s.Name(), err)
			}
		})
		r.queues = append(r.queues, q)
	}
	return r
}

// Route sends line to every sink queue.
// Lines are dropped per-sink if its queue is full (backpressure).
func (r *Router) Route(ctx context.Context, line string) error {
	var errs []string
	for i, q := range r.queues {
		if !q.Enqueue(ctx, line) {
			errs = append(errs, fmt.Sprintf("sink %s: queue full or ctx done", r.sinks[i].Name()))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("router dropped lines: %v", errs)
	}
	return nil
}

// Close drains all sink queues and closes every sink.
func (r *Router) Close() error {
	for _, q := range r.queues {
		q.Close()
	}
	for _, s := range r.sinks {
		if err := s.Close(); err != nil {
			log.Printf("[router] sink %s close error: %v", s.Name(), err)
		}
	}
	return nil
}
