package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestMultilineAggregator_WithBoundedQueue verifies that the multiline
// aggregator works end-to-end with BoundedQueue as the upstream source.
func TestMultilineAggregator_WithBoundedQueue(t *testing.T) {
	cap := &captureMultilineSink{}
	cfg := MultilineConfig{
		StartPattern: "LOG:",
		FlushTimeout: 200 * time.Millisecond,
		MaxLines:     50,
	}
	m, err := NewMultilineAggregator(cfg, cap)
	if err != nil {
		t.Fatal(err)
	}

	q := NewBoundedQueue(DefaultBackpressureConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Produce lines into the queue.
	lines := []string{
		"LOG: first event",
		"  continuation A",
		"  continuation B",
		"LOG: second event",
		"  continuation C",
	}
	for _, l := range lines {
		if err := q.Enqueue(ctx, l); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	// Drain via multiline filter.
	in := make(chan string, len(lines))
	out := make(chan string, len(lines))
	for _, l := range lines {
		in <- l
	}
	close(in)

	filterDone := make(chan struct{})
	go func() {
		m.Filter(ctx, in, out)
		close(out)
		close(filterDone)
	}()

	select {
	case <-filterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("filter did not finish in time")
	}

	var events []string
	for e := range out {
		events = append(events, e)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 aggregated events, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "continuation A") {
		t.Errorf("first event missing continuation: %s", events[0])
	}
	if !strings.Contains(events[1], "continuation C") {
		t.Errorf("second event missing continuation: %s", events[1])
	}
}
