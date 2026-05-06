package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestThrottleSink_WithBoundedQueue verifies that the throttle sink integrates
// correctly with BoundedQueue: only the first line per interval reaches the
// inner sink even when many lines are enqueued rapidly.
func TestThrottleSink_WithBoundedQueue(t *testing.T) {
	inner := &captureSink{name: "stdout"}
	th, err := NewThrottleSink(inner, ThrottleConfig{Interval: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	q := NewBoundedQueue(DefaultBackpressureConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-q.Out():
				if !ok {
					return
				}
				_ = th.Write(ctx, line)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		q.Enqueue(ctx, "repeated-line")
	}

	// Give the consumer goroutine time to drain.
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	if inner.count != 1 {
		t.Fatalf("expected exactly 1 write through throttle, got %d", inner.count)
	}
}
