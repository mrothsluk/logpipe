package pipeline

import (
	"context"
	"testing"
	"time"
)

// TestSampler_WithBoundedQueue verifies that the Sampler integrates correctly
// with the BoundedQueue: lines flow through the queue, get sampled, and are
// delivered to the output channel without deadlock.
func TestSampler_WithBoundedQueue(t *testing.T) {
	const total = 200

	q := NewBoundedQueue(DefaultBackpressureConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		for i := 0; i < total; i++ {
			if err := q.Enqueue(ctx, "integration-line"); err != nil {
				return
			}
		}
		q.Close()
	}()

	// Drain the queue into a channel that the sampler can read.
	mid := make(chan string, 64)
	go func() {
		defer close(mid)
		q.Drain(ctx, func(line string) {
			select {
			case mid <- line:
			case <-ctx.Done():
			}
		})
	}()

	s, err := NewSampler(SamplingConfig{Rate: 0.5}, 7)
	if err != nil {
		t.Fatalf("NewSampler: %v", err)
	}

	out := make(chan string, total)
	s.Filter(ctx, mid, out)
	close(out)

	var kept int
	for range out {
		kept++
	}

	if kept == 0 {
		t.Fatal("expected some lines to pass through sampler")
	}
	if kept == total {
		t.Fatal("expected sampler to drop some lines at rate=0.5")
	}
	t.Logf("kept %d/%d lines through sampler+queue integration", kept, total)
}
