package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/logpipe/internal/pipeline"
)

func TestHeaderSink_WithBoundedQueue(t *testing.T) {
	inner := &captureSink{}
	cfg := pipeline.HeaderConfig{Header: "### LOG START ###"}
	h, err := pipeline.NewHeaderSink(inner, cfg)
	if err != nil {
		t.Fatalf("NewHeaderSink: %v", err)
	}

	qCfg := pipeline.DefaultBackpressureConfig()
	qCfg.Capacity = 16
	q := pipeline.NewBoundedQueue(qCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go q.Drain(ctx, func(line string) {
		_ = h.Write(ctx, line)
	})

	lines := []string{"alpha", "beta", "gamma"}
	for _, l := range lines {
		if !q.Enqueue(l) {
			t.Fatalf("queue full, could not enqueue %q", l)
		}
	}

	time.Sleep(100 * time.Millisecond)
	cancel()
	q.Close()

	// Expect header + 3 lines = 4 entries
	if len(inner.lines) != 4 {
		t.Fatalf("expected 4 lines (header + 3), got %d: %v", len(inner.lines), inner.lines)
	}
	if inner.lines[0] != "### LOG START ###" {
		t.Errorf("first line should be header, got %q", inner.lines[0])
	}
}
