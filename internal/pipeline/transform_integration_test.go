package pipeline_test

import (
	"strings"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
)

// TestTransformer_WithBoundedQueue verifies that transformed lines flow
// correctly through a BoundedQueue end-to-end.
func TestTransformer_WithBoundedQueue(t *testing.T) {
	cfg := pipeline.DefaultBackpressureConfig()
	cfg.Capacity = 16
	q := pipeline.NewBoundedQueue(cfg)

	tr := pipeline.NewTransformer(pipeline.TransformConfig{
		StripANSI: true,
		Prefix:    "[test] ",
	})

	rawLines := []string{
		"\x1b[31mERROR\x1b[0m: disk full",
		"\x1b[32mINFO\x1b[0m: all good",
		"plain message",
	}

	for _, raw := range rawLines {
		line, keep := tr.Apply(raw)
		if !keep {
			continue
		}
		if dropped := q.Enqueue(line); dropped {
			t.Fatalf("unexpected drop for line: %q", line)
		}
	}

	q.Close()

	var received []string
	for line := range q.Out() {
		received = append(received, line)
	}

	if len(received) != len(rawLines) {
		t.Fatalf("expected %d lines, got %d", len(rawLines), len(received))
	}

	for _, line := range received {
		if !strings.HasPrefix(line, "[test] ") {
			t.Errorf("missing prefix on line: %q", line)
		}
		if strings.ContainsRune(line, '\x1b') {
			t.Errorf("ANSI escape not stripped from line: %q", line)
		}
	}
}
