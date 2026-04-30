package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockSink struct {
	name       string
	callCount  int
	failUntil  int
	closed     bool
	writtenLines []string
}

func (m *mockSink) Name() string { return m.name }
func (m *mockSink) Close() error { m.closed = true; return nil }
func (m *mockSink) Write(_ context.Context, line string) error {
	m.callCount++
	if m.callCount <= m.failUntil {
		return errors.New("mock write error")
	}
	m.writtenLines = append(m.writtenLines, line)
	return nil
}

func noSleep(_ time.Duration) {}

func TestRetrySink_Name(t *testing.T) {
	ms := &mockSink{name: "stdout"}
	rs := NewRetrySink(ms, DefaultRetryConfig())
	if rs.Name() != "retry(stdout)" {
		t.Errorf("unexpected name: %s", rs.Name())
	}
}

func TestRetrySink_SucceedsOnFirstAttempt(t *testing.T) {
	ms := &mockSink{name: "test", failUntil: 0}
	rs := NewRetrySink(ms, DefaultRetryConfig())
	rs.sleepFn = noSleep
	if err := rs.Write(context.Background(), "hello"); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ms.callCount != 1 {
		t.Errorf("expected 1 call, got %d", ms.callCount)
	}
}

func TestRetrySink_RetriesOnFailure(t *testing.T) {
	ms := &mockSink{name: "test", failUntil: 2}
	cfg := DefaultRetryConfig()
	rs := NewRetrySink(ms, cfg)
	rs.sleepFn = noSleep
	if err := rs.Write(context.Background(), "line"); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if ms.callCount != 3 {
		t.Errorf("expected 3 calls, got %d", ms.callCount)
	}
}

func TestRetrySink_ExhaustsMaxAttempts(t *testing.T) {
	ms := &mockSink{name: "test", failUntil: 10}
	cfg := DefaultRetryConfig() // MaxAttempts = 3
	rs := NewRetrySink(ms, cfg)
	rs.sleepFn = noSleep
	if err := rs.Write(context.Background(), "line"); err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if ms.callCount != 3 {
		t.Errorf("expected 3 calls, got %d", ms.callCount)
	}
}

func TestRetrySink_ContextCancelStopsRetry(t *testing.T) {
	ms := &mockSink{name: "test", failUntil: 10}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rs := NewRetrySink(ms, DefaultRetryConfig())
	rs.sleepFn = noSleep
	if err := rs.Write(ctx, "line"); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestRetrySink_Close(t *testing.T) {
	ms := &mockSink{name: "test"}
	rs := NewRetrySink(ms, DefaultRetryConfig())
	if err := rs.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !ms.closed {
		t.Error("expected inner sink to be closed")
	}
}
