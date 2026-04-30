package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/logpipe/internal/pipeline"
)

// TestFilter_WithBoundedQueue verifies that Filter integrates correctly
// with BoundedQueue: only matching lines are forwarded downstream.
func TestFilter_WithBoundedQueue(t *testing.T) {
	cap := &captureFilter{}

	filter, err := pipeline.NewFilter(pipeline.FilterConfig{IncludePattern: `WARN|ERROR`}, cap)
	if err != nil {
		t.Fatalf("NewFilter: %v", err)
	}

	q := pipeline.NewBoundedQueue(pipeline.DefaultBackpressureConfig(), filter)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go q.Drain(ctx)

	input := []string{
		"INFO: startup",
		"WARN: low memory",
		"DEBUG: verbose",
		"ERROR: crash",
		"INFO: shutdown",
	}
	for _, line := range input {
		if err := q.Enqueue(ctx, line); err != nil {
			t.Fatalf("Enqueue(%q): %v", line, err)
		}
	}

	// Give drain loop time to process.
	time.Sleep(100 * time.Millisecond)
	cancel()

	if len(cap.lines) != 2 {
		t.Errorf("expected 2 lines (WARN+ERROR), got %d: %v", len(cap.lines), cap.lines)
	}
}
