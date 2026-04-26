package pipeline

import (
	"errors"
	"testing"
)

// mockSink is a test double for sink.Sink.
type mockSink struct {
	name    string
	lines   []string
	writeErr error
	closed  bool
}

func (m *mockSink) Name() string { return m.name }
func (m *mockSink) Write(line string) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.lines = append(m.lines, line)
	return nil
}
func (m *mockSink) Close() error {
	m.closed = true
	return nil
}

func TestRouter_Route_AllSinks(t *testing.T) {
	a := &mockSink{name: "a"}
	b := &mockSink{name: "b"}
	r := NewRouter([]sink.Sink{a, b})

	if err := r.Route("hello"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(a.lines) != 1 || a.lines[0] != "hello" {
		t.Errorf("sink a: expected [hello], got %v", a.lines)
	}
	if len(b.lines) != 1 || b.lines[0] != "hello" {
		t.Errorf("sink b: expected [hello], got %v", b.lines)
	}
}

func TestRouter_Route_ContinuesOnError(t *testing.T) {
	a := &mockSink{name: "a", writeErr: errors.New("boom")}
	b := &mockSink{name: "b"}
	r := NewRouter([]sink.Sink{a, b})

	err := r.Route("line")
	if err == nil {
		t.Fatal("expected error from failing sink")
	}
	// b should still receive the line
	if len(b.lines) != 1 {
		t.Errorf("sink b should have received the line despite sink a failing")
	}
}

func TestRouter_Close_AllSinks(t *testing.T) {
	a := &mockSink{name: "a"}
	b := &mockSink{name: "b"}
	r := NewRouter([]sink.Sink{a, b})

	if err := r.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !a.closed || !b.closed {
		t.Error("expected both sinks to be closed")
	}
}

func TestRouter_Sinks_ReturnsAll(t *testing.T) {
	a := &mockSink{name: "a"}
	r := NewRouter([]sink.Sink{a})
	if len(r.Sinks()) != 1 {
		t.Errorf("expected 1 sink, got %d", len(r.Sinks()))
	}
}
