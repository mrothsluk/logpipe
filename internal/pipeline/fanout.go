package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/yourorg/logpipe/internal/sink"
)

// Fanout distributes log lines from a single input channel to multiple sinks
// concurrently, respecting backpressure via BoundedQueues per sink.
type Fanout struct {
	sinks  []sink.Sink
	queues []*BoundedQueue
	wg     sync.WaitGroup
}

// NewFanout creates a Fanout that writes to the given sinks. Each sink gets
// its own BoundedQueue with the provided backpressure config.
func NewFanout(sinks []sink.Sink, cfg BackpressureConfig) *Fanout {
	queues := make([]*BoundedQueue, len(sinks))
	for i := range sinks {
		queues[i] = NewBoundedQueue(cfg)
	}
	return &Fanout{
		sinks:  sinks,
		queues: queues,
	}
}

// Start begins draining each sink's queue in a dedicated goroutine.
// It returns when the context is cancelled or all queues are closed.
func (f *Fanout) Start(ctx context.Context) {
	for i, s := range f.sinks {
		f.wg.Add(1)
		go func(q *BoundedQueue, sk sink.Sink) {
			defer f.wg.Done()
			for {
				line, ok := q.Dequeue(ctx)
				if !ok {
					return
				}
				if err := sk.Write(line); err != nil {
					fmt.Printf("fanout: sink %s write error: %v\n", sk.Name(), err)
				}
			}
		}(f.queues[i], s)
	}
}

// Send enqueues a log line to all sink queues. Lines may be dropped per
// the BoundedQueue drop policy if a queue is full.
func (f *Fanout) Send(ctx context.Context, line string) {
	for _, q := range f.queues {
		q.Enqueue(ctx, line)
	}
}

// Close shuts down all queues and waits for drain goroutines to finish.
func (f *Fanout) Close() {
	for _, q := range f.queues {
		q.Close()
	}
	f.wg.Wait()
}
