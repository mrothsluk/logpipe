package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
)

func TestHeaderSink_NilInner(t *testing.T) {
	_, err := pipeline.NewHeaderSink(nil, pipeline.DefaultHeaderConfig())
	if err == nil {
		t.Fatal("expected error for nil inner sink")
	}
}

func TestHeaderSink_EmptyHeader(t *testing.T) {
	cfg := pipeline.HeaderConfig{Header: "   "}
	_, err := pipeline.NewHeaderSink(&captureSink{}, cfg)
	if err == nil {
		t.Fatal("expected error for empty header")
	}
}

func TestHeaderSink_Name(t *testing.T) {
	inner := &captureSink{name: "stdout"}
	cfg := pipeline.HeaderConfig{Header: "---"}
	h, _ := pipeline.NewHeaderSink(inner, cfg)
	if h.Name() != "header(stdout)" {
		t.Fatalf("unexpected name: %s", h.Name())
	}
}

func TestHeaderSink_EmitsHeaderBeforeFirstLine(t *testing.T) {
	inner := &captureSink{}
	cfg := pipeline.HeaderConfig{Header: "=== START ==="}
	h, _ := pipeline.NewHeaderSink(inner, cfg)

	_ = h.Write(context.Background(), "line1")
	_ = h.Write(context.Background(), "line2")

	if len(inner.lines) != 3 {
		t.Fatalf("expected 3 lines (header + 2), got %d", len(inner.lines))
	}
	if inner.lines[0] != "=== START ===" {
		t.Errorf("expected header first, got %q", inner.lines[0])
	}
}

func TestHeaderSink_RepeatEvery(t *testing.T) {
	inner := &captureSink{}
	cfg := pipeline.HeaderConfig{Header: "---", RepeatEvery: 2}
	h, _ := pipeline.NewHeaderSink(inner, cfg)

	for i := 0; i < 4; i++ {
		_ = h.Write(context.Background(), "line")
	}
	// header at 0, repeat at count==2 (before 3rd line), repeat at count==4 (before 5th – not reached)
	// lines: header, line, line, header, line, line  => 6
	if len(inner.lines) != 6 {
		t.Fatalf("expected 6 lines, got %d: %v", len(inner.lines), inner.lines)
	}
}

func TestHeaderSink_Close(t *testing.T) {
	inner := &captureSink{}
	cfg := pipeline.HeaderConfig{Header: "hdr"}
	h, _ := pipeline.NewHeaderSink(inner, cfg)
	if err := h.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestHeaderSink_InnerWriteError_Propagated(t *testing.T) {
	expected := errors.New("write failed")
	inner := &captureSink{writeErr: expected}
	cfg := pipeline.HeaderConfig{Header: "hdr"}
	h, _ := pipeline.NewHeaderSink(inner, cfg)

	err := h.Write(context.Background(), "line")
	if !errors.Is(err, expected) {
		t.Fatalf("expected propagated error, got %v", err)
	}
}
