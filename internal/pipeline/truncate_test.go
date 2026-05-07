package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
)

func TestTruncateSink_NilInner(t *testing.T) {
	_, err := pipeline.NewTruncateSink(nil, pipeline.DefaultTruncateConfig())
	if err == nil {
		t.Fatal("expected error for nil inner sink")
	}
}

func TestTruncateSink_ZeroMaxBytes(t *testing.T) {
	cfg := pipeline.TruncateConfig{MaxBytes: 0, Suffix: "..."}
	_, err := pipeline.NewTruncateSink(&mockSink{name: "mock"}, cfg)
	if err == nil {
		t.Fatal("expected error for zero MaxBytes")
	}
}

func TestTruncateSink_Name(t *testing.T) {
	cfg := pipeline.DefaultTruncateConfig()
	s, err := pipeline.NewTruncateSink(&mockSink{name: "stdout"}, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name() != "truncate(stdout)" {
		t.Errorf("unexpected name: %s", s.Name())
	}
}

func TestTruncateSink_ShortLine_PassedThrough(t *testing.T) {
	inner := &mockSink{name: "mock"}
	cfg := pipeline.TruncateConfig{MaxBytes: 20, Suffix: "...[truncated]"}
	s, _ := pipeline.NewTruncateSink(inner, cfg)

	_ = s.Write(context.Background(), "hello")
	if inner.lastLine != "hello" {
		t.Errorf("expected 'hello', got %q", inner.lastLine)
	}
}

func TestTruncateSink_LongLine_Truncated(t *testing.T) {
	inner := &mockSink{name: "mock"}
	cfg := pipeline.TruncateConfig{MaxBytes: 10, Suffix: "..."}
	s, _ := pipeline.NewTruncateSink(inner, cfg)

	_ = s.Write(context.Background(), "abcdefghijklmnop")
	expected := "abcdefg..."
	if inner.lastLine != expected {
		t.Errorf("expected %q, got %q", expected, inner.lastLine)
	}
}

func TestTruncateSink_DefaultSuffix_Applied(t *testing.T) {
	inner := &mockSink{name: "mock"}
	cfg := pipeline.TruncateConfig{MaxBytes: 20, Suffix: ""}
	s, _ := pipeline.NewTruncateSink(inner, cfg)

	long := "this line is definitely longer than twenty bytes"
	_ = s.Write(context.Background(), long)
	if len(inner.lastLine) > 20 {
		t.Errorf("line not truncated to MaxBytes: len=%d", len(inner.lastLine))
	}
}

func TestTruncateSink_Close_DelegatesToInner(t *testing.T) {
	inner := &mockSink{name: "mock"}
	s, _ := pipeline.NewTruncateSink(inner, pipeline.DefaultTruncateConfig())
	if err := s.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !inner.closed {
		t.Error("expected inner sink to be closed")
	}
}

func TestTruncateSink_InnerWriteError_Propagated(t *testing.T) {
	expectedErr := errors.New("write failed")
	inner := &mockSink{name: "mock", writeErr: expectedErr}
	s, _ := pipeline.NewTruncateSink(inner, pipeline.DefaultTruncateConfig())

	err := s.Write(context.Background(), "some line")
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}
