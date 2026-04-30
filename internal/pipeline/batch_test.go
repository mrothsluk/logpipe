package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// collectSink records every Write call.
type collectSink struct {
	mu    sync.Mutex
	lines []string
}

func (c *collectSink) Name() string { return "collect" }
func (c *collectSink) Write(_ context.Context, line string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
	return nil
}
func (c *collectSink) Close() error { return nil }
func (c *collectSink) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.lines))
	copy(out, c.lines)
	return out
}

func TestBatcher_NilSink(t *testing.T) {
	_, err := NewBatcher(DefaultBatchConfig(), nil)
	if err == nil {
		t.Fatal("expected error for nil sink")
	}
}

func TestBatcher_FlushOnMaxSize(t *testing.T) {
	s := &collectSink{}
	cfg := BatchConfig{MaxSize: 3, MaxDelay: 10 * time.Second}
	b, err := NewBatcher(cfg, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go b.Run(ctx, in)

	in <- "a"
	in <- "b"
	in <- "c" // triggers size-based flush

	time.Sleep(50 * time.Millisecond)
	got := s.snapshot()
	if len(got) != 3 {
		t.Fatalf("expected 3 lines flushed, got %d", len(got))
	}
}

func TestBatcher_FlushOnDelay(t *testing.T) {
	s := &collectSink{}
	cfg := BatchConfig{MaxSize: 100, MaxDelay: 50 * time.Millisecond}
	b, err := NewBatcher(cfg, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go b.Run(ctx, in)

	in <- "hello"
	time.Sleep(150 * time.Millisecond)

	got := s.snapshot()
	if len(got) != 1 || got[0] != "hello" {
		t.Fatalf("expected 1 flushed line, got %v", got)
	}
}

func TestBatcher_FlushOnChannelClose(t *testing.T) {
	s := &collectSink{}
	cfg := BatchConfig{MaxSize: 100, MaxDelay: 10 * time.Second}
	b, _ := NewBatcher(cfg, s)

	in := make(chan string, 5)
	in <- "x"
	in <- "y"
	close(in)

	b.Run(context.Background(), in)

	got := s.snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 lines after channel close, got %d", len(got))
	}
}

func TestBatcher_DefaultConfigApplied(t *testing.T) {
	s := &collectSink{}
	cfg := BatchConfig{MaxSize: 0, MaxDelay: 0} // zeros → defaults
	b, err := NewBatcher(cfg, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.cfg.MaxSize != DefaultBatchConfig().MaxSize {
		t.Errorf("expected default MaxSize %d, got %d", DefaultBatchConfig().MaxSize, b.cfg.MaxSize)
	}
	if b.cfg.MaxDelay != DefaultBatchConfig().MaxDelay {
		t.Errorf("expected default MaxDelay %v, got %v", DefaultBatchConfig().MaxDelay, b.cfg.MaxDelay)
	}
}
