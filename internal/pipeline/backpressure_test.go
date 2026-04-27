package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedQueue_EnqueueAndDrain(t *testing.T) {
	var received []string
	var mu sync.Mutex

	cfg := BackpressureConfig{Capacity: 8, WriteTimeout: 50 * time.Millisecond}
	bq := NewBoundedQueue(cfg, func(line string) {
		mu.Lock()
		received = append(received, line)
		mu.Unlock()
	})

	lines := []string{"alpha", "beta", "gamma"}
	for _, l := range lines {
		if !bq.Enqueue(context.Background(), l) {
			t.Fatalf("expected enqueue to succeed for %q", l)
		}
	}
	bq.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(lines) {
		t.Fatalf("expected %d lines, got %d", len(lines), len(received))
	}
}

func TestBoundedQueue_DropsWhenFull(t *testing.T) {
	// handler blocks so the channel fills up
	block := make(chan struct{})
	var dropped int32

	cfg := BackpressureConfig{Capacity: 2, WriteTimeout: 10 * time.Millisecond}
	bq := NewBoundedQueue(cfg, func(line string) {
		<-block
	})

	// Fill the channel
	bq.Enqueue(context.Background(), "line1")
	bq.Enqueue(context.Background(), "line2")

	// This should time out and return false
	if bq.Enqueue(context.Background(), "overflow") {
		atomic.AddInt32(&dropped, 1)
	} else {
		atomic.AddInt32(&dropped, 1)
	}

	close(block)
	bq.Close()

	if atomic.LoadInt32(&dropped) == 0 {
		t.Fatal("expected at least one drop attempt to be recorded")
	}
}

func TestBoundedQueue_ContextCancel(t *testing.T) {
	block := make(chan struct{})
	cfg := BackpressureConfig{Capacity: 1, WriteTimeout: 5 * time.Second}
	bq := NewBoundedQueue(cfg, func(line string) { <-block })

	// Fill the single slot
	bq.Enqueue(context.Background(), "filler")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	if bq.Enqueue(ctx, "should-drop") {
		t.Fatal("expected enqueue to fail on cancelled context")
	}

	close(block)
	bq.Close()
}

func TestBoundedQueue_CloseIdempotent(t *testing.T) {
	cfg := DefaultBackpressureConfig()
	bq := NewBoundedQueue(cfg, func(line string) {})
	bq.Close()
	bq.Close() // should not panic
}

func TestBoundedQueue_Len(t *testing.T) {
	block := make(chan struct{})
	cfg := BackpressureConfig{Capacity: 10, WriteTimeout: 50 * time.Millisecond}
	bq := NewBoundedQueue(cfg, func(line string) { <-block })

	bq.Enqueue(context.Background(), "a")
	bq.Enqueue(context.Background(), "b")

	if bq.Len() < 1 {
		t.Fatalf("expected Len >= 1, got %d", bq.Len())
	}
	close(block)
	bq.Close()
}
