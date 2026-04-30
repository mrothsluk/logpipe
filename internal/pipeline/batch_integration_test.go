package pipeline

import (
	"context"
	"testing"
	"time"
)

// TestBatcher_WithBoundedQueue verifies that the Batcher drains a BoundedQueue
// correctly, flushing all enqueued lines to the sink.
func TestBatcher_WithBoundedQueue(t *testing.T) {
	const total = 25

	s := &collectSink{}
	cfg := BatchConfig{MaxSize: 10, MaxDelay: 50 * time.Millisecond}
	b, err := NewBatcher(cfg, s)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}

	qCfg := DefaultBackpressureConfig()
	qCfg.Capacity = 64
	q := NewBoundedQueue(qCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Feed lines into the queue.
	for i := 0; i < total; i++ {
		if err := q.Enqueue(ctx, "line"); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	q.Close()

	// Drain the queue into a plain channel the batcher can consume.
	ch := make(chan string, total)
	go func() {
		defer close(ch)
		for {
			line, ok := q.Dequeue(ctx)
			if !ok {
				return
			}
			ch <- line
		}
	}()

	b.Run(ctx, ch)

	got := s.snapshot()
	if len(got) != total {
		t.Fatalf("expected %d lines written, got %d", total, len(got))
	}
}
