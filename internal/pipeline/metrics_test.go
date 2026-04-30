package pipeline

import (
	"context"
	"testing"
)

func TestMetrics_InitialValuesAreZero(t *testing.T) {
	var m Metrics
	s := m.Snapshot()
	if s.LinesIn != 0 || s.LinesOut != 0 || s.LinesDropped != 0 || s.LinesFiltered != 0 || s.Errors != 0 {
		t.Fatalf("expected all zeros, got %+v", s)
	}
}

func TestMetrics_Reset(t *testing.T) {
	var m Metrics
	m.LinesIn.Add(10)
	m.LinesDropped.Add(3)
	m.Errors.Add(1)
	m.Reset()
	s := m.Snapshot()
	if s.LinesIn != 0 || s.LinesDropped != 0 || s.Errors != 0 {
		t.Fatalf("expected zeros after reset, got %+v", s)
	}
}

func TestInstrumentedQueue_EnqueueAccepted(t *testing.T) {
	cfg := DefaultBackpressureConfig()
	cfg.Capacity = 10
	q := NewBoundedQueue(cfg)
	var m Metrics
	iq := NewInstrumentedQueue(q, &m)

	for i := 0; i < 5; i++ {
		if !iq.Enqueue("line") {
			t.Fatalf("expected enqueue to succeed on iteration %d", i)
		}
	}

	s := m.Snapshot()
	if s.LinesIn != 5 {
		t.Errorf("expected LinesIn=5, got %d", s.LinesIn)
	}
	if s.LinesDropped != 0 {
		t.Errorf("expected LinesDropped=0, got %d", s.LinesDropped)
	}
}

func TestInstrumentedQueue_EnqueueDropped(t *testing.T) {
	cfg := DefaultBackpressureConfig()
	cfg.Capacity = 2
	cfg.DropOnFull = true
	q := NewBoundedQueue(cfg)
	var m Metrics
	iq := NewInstrumentedQueue(q, &m)

	// Fill the queue
	iq.Enqueue("a")
	iq.Enqueue("b")
	// This should be dropped
	iq.Enqueue("c")

	s := m.Snapshot()
	if s.LinesIn != 3 {
		t.Errorf("expected LinesIn=3, got %d", s.LinesIn)
	}
	if s.LinesDropped != 1 {
		t.Errorf("expected LinesDropped=1, got %d", s.LinesDropped)
	}
}

func TestInstrumentedQueue_NilMetricsAllocated(t *testing.T) {
	cfg := DefaultBackpressureConfig()
	q := NewBoundedQueue(cfg)
	iq := NewInstrumentedQueue(q, nil)
	if iq.Metrics() == nil {
		t.Fatal("expected non-nil Metrics when nil passed to constructor")
	}
	iq.Enqueue("hello")
	if iq.Metrics().Snapshot().LinesIn != 1 {
		t.Error("expected LinesIn=1")
	}
}

func TestInstrumentedQueue_QueueAccessor(t *testing.T) {
	cfg := DefaultBackpressureConfig()
	q := NewBoundedQueue(cfg)
	iq := NewInstrumentedQueue(q, nil)
	iq.Enqueue("msg")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := iq.Queue().Drain(ctx)
	line := <-ch
	if line != "msg" {
		t.Errorf("expected 'msg', got %q", line)
	}
}
