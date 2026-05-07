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
