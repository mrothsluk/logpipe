package sink_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/yourorg/logpipe/internal/sink"
)

func TestStdoutSink_Name(t *testing.T) {
	s := sink.NewStdoutSink(nil, "")
	if s.Name() != "stdout" {
		t.Fatalf("expected name 'stdout', got %q", s.Name())
	}
}

func TestStdoutSink_Write_NoPrefix(t *testing.T) {
	var buf bytes.Buffer
	s := sink.NewStdoutSink(&buf, "")

	if err := s.Write(context.Background(), "hello world"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := strings.TrimRight(buf.String(), "\n")
	if got != "hello world" {
		t.Fatalf("expected 'hello world', got %q", got)
	}
}

func TestStdoutSink_Write_WithPrefix(t *testing.T) {
	var buf bytes.Buffer
	s := sink.NewStdoutSink(&buf, "[app]")

	if err := s.Write(context.Background(), "startup complete"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := strings.TrimRight(buf.String(), "\n")
	if got != "[app] startup complete" {
		t.Fatalf("expected '[app] startup complete', got %q", got)
	}
}

func TestStdoutSink_Close(t *testing.T) {
	s := sink.NewStdoutSink(nil, "")
	if err := s.Close(); err != nil {
		t.Fatalf("Close returned unexpected error: %v", err)
	}
}

func TestStdoutSink_Write_ConcurrentSafe(t *testing.T) {
	var buf bytes.Buffer
	s := sink.NewStdoutSink(&buf, "")
	ctx := context.Background()

	const goroutines = 20
	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			_ = s.Write(ctx, "concurrent line")
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != goroutines {
		t.Fatalf("expected %d lines, got %d", goroutines, len(lines))
	}
}
