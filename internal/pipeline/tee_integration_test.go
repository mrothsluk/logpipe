package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/logpipe/internal/pipeline"
)

// TestTeeSink_WithBoundedQueue verifies that a TeeSink works correctly when
// lines are delivered via a BoundedQueue, ensuring the tee fanout is
// compatible with the backpressure layer.
func TestTeeSink_WithBoundedQueue(t *testing.T) {
	primary := &recordSink{name: "primary"}
	secondary := &recordSink{name: "secondary"}

	tee, err := pipeline.NewTeeSink(primary, secondary)
	if err != nil {
		t.Fatalf("NewTeeSink: %v", err)
	}

	cfg := pipeline.DefaultBackpressureConfig()
	cfg.Capacity = 16
	q := pipeline.NewBoundedQueue(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const lines = 5
	go func() {
		for i := 0; i < lines; i++ {
			q.Enqueue("integration line")
		}
		q.Close()
	}()

	q.Drain(ctx, func(line string) {
		if err := tee.Write(ctx, line); err != nil {
			t.Errorf("tee.Write: %v", err)
		}
	})

	if len(primary.lines) != lines {
		t.Errorf("primary: want %d lines, got %d", lines, len(primary.lines))
	}
	if len(secondary.lines) != lines {
		t.Errorf("secondary: want %d lines, got %d", lines, len(secondary.lines))
	}
}

// TestTeeSink_WithBoundedQueue_ContentMatch verifies that the actual line
// content received by each sink matches what was enqueued, not just the count.
func TestTeeSink_WithBoundedQueue_ContentMatch(t *testing.T) {
	primary := &recordSink{name: "primary"}
	secondary := &recordSink{name: "secondary"}

	tee, err := pipeline.NewTeeSink(primary, secondary)
	if err != nil {
		t.Fatalf("NewTeeSink: %v", err)
	}

	cfg := pipeline.DefaultBackpressureConfig()
	cfg.Capacity = 16
	q := pipeline.NewBoundedQueue(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	want := []string{"alpha", "beta", "gamma"}
	go func() {
		for _, line := range want {
			q.Enqueue(line)
		}
		q.Close()
	}()

	q.Drain(ctx, func(line string) {
		if err := tee.Write(ctx, line); err != nil {
			t.Errorf("tee.Write: %v", err)
		}
	})

	for i, got := range primary.lines {
		if got != want[i] {
			t.Errorf("primary line %d: want %q, got %q", i, want[i], got)
		}
	}
	for i, got := range secondary.lines {
		if got != want[i] {
			t.Errorf("secondary line %d: want %q, got %q", i, want[i], got)
		}
	}
}
