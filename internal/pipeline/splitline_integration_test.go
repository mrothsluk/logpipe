package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/logpipe/internal/pipeline"
)

// TestSplitLineSink_WithBoundedQueue verifies that SplitLineSink works
// correctly when lines are delivered through a BoundedQueue.
func TestSplitLineSink_WithBoundedQueue(t *testing.T) {
	cap := &captureSink{name: "cap"}

	splitter, err := pipeline.NewSplitLineSink(
		pipeline.SplitLineConfig{Delimiter: ";", TrimSpace: true, SkipEmpty: true},
		cap,
	)
	if err != nil {
		t.Fatal(err)
	}

	q := pipeline.NewBoundedQueue(pipeline.DefaultBackpressureConfig(), splitter)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go q.Drain(ctx)

	inputs := []string{
		"alpha; beta; gamma",
		"one;two",
		";;",
	}
	for _, line := range inputs {
		if !q.Enqueue(line) {
			t.Fatalf("failed to enqueue: %q", line)
		}
	}

	// allow drain to process
	time.Sleep(100 * time.Millisecond)
	cancel()

	// alpha;beta;gamma → 3, one;two → 2, ;; → 0 (all empty after trim+skip)
	expected := 5
	if len(cap.lines) != expected {
		t.Fatalf("expected %d forwarded segments, got %d: %v", expected, len(cap.lines), cap.lines)
	}
}
