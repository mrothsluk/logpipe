package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// recordSink captures written lines for assertions.
type recordSink struct {
	mu    sync.Mutex
	name  string
	lines []string
}

func (r *recordSink) Name() string { return r.name }
func (r *recordSink) Write(line string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, line)
	return nil
}
func (r *recordSink) Close() error { return nil }

func (r *recordSink) Lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.lines))
	copy(out, r.lines)
	return out
}

func TestFanout_SendToAllSinks(t *testing.T) {
	a := &recordSink{name: "a"}
	b := &recordSink{name: "b"}
	cfg := DefaultBackpressureConfig()
	fo := NewFanout([]Sink{a, b}, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fo.Start(ctx)

	fo.Send(ctx, "hello")
	fo.Send(ctx, "world")

	fo.Close()

	for _, s := range []*recordSink{a, b} {
		lines := s.Lines()
		if len(lines) != 2 {
			t.Errorf("sink %s: expected 2 lines, got %d", s.Name(), len(lines))
		}
	}
}

func TestFanout_ContextCancelStopsGoroutines(t *testing.T) {
	a := &recordSink{name: "a"}
	cfg := DefaultBackpressureConfig()
	fo := NewFanout([]Sink{a}, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	fo.Start(ctx)
	cancel()

	done := make(chan struct{})
	go func() {
		fo.Close()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("fanout did not shut down within timeout")
	}
}

func TestFanout_NoSinks(t *testing.T) {
	cfg := DefaultBackpressureConfig()
	fo := NewFanout(nil, cfg)
	ctx := context.Background()
	fo.Start(ctx)
	fo.Send(ctx, "line") // should not panic
	fo.Close()
}
