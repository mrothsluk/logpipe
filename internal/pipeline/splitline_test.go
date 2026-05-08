package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
)

func TestSplitLineSink_NilInner(t *testing.T) {
	_, err := pipeline.NewSplitLineSink(pipeline.DefaultSplitLineConfig(), nil)
	if err == nil {
		t.Fatal("expected error for nil inner sink")
	}
}

func TestSplitLineSink_Name(t *testing.T) {
	s, _ := pipeline.NewSplitLineSink(pipeline.DefaultSplitLineConfig(), &captureSink{name: "stdout"})
	if s.Name() != "splitline(stdout)" {
		t.Fatalf("unexpected name: %s", s.Name())
	}
}

func TestSplitLineSink_DefaultDelimiter(t *testing.T) {
	cfg := pipeline.SplitLineConfig{} // empty delimiter → should default to ","
	cap := &captureSink{name: "cap"}
	s, err := pipeline.NewSplitLineSink(cfg, cap)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(context.Background(), "a,b,c")
	if len(cap.lines) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(cap.lines))
	}
}

func TestSplitLineSink_SplitsOnDelimiter(t *testing.T) {
	cfg := pipeline.SplitLineConfig{Delimiter: "|", TrimSpace: false, SkipEmpty: false}
	cap := &captureSink{name: "cap"}
	s, _ := pipeline.NewSplitLineSink(cfg, cap)
	_ = s.Write(context.Background(), "foo|bar|baz")
	if len(cap.lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(cap.lines))
	}
	if cap.lines[1] != "bar" {
		t.Fatalf("unexpected segment: %q", cap.lines[1])
	}
}

func TestSplitLineSink_TrimSpace(t *testing.T) {
	cfg := pipeline.SplitLineConfig{Delimiter: ",", TrimSpace: true, SkipEmpty: false}
	cap := &captureSink{name: "cap"}
	s, _ := pipeline.NewSplitLineSink(cfg, cap)
	_ = s.Write(context.Background(), " hello , world ")
	if cap.lines[0] != "hello" || cap.lines[1] != "world" {
		t.Fatalf("trim failed: %v", cap.lines)
	}
}

func TestSplitLineSink_SkipEmpty(t *testing.T) {
	cfg := pipeline.SplitLineConfig{Delimiter: ",", TrimSpace: true, SkipEmpty: true}
	cap := &captureSink{name: "cap"}
	s, _ := pipeline.NewSplitLineSink(cfg, cap)
	_ = s.Write(context.Background(), "a,,b,,c")
	if len(cap.lines) != 3 {
		t.Fatalf("expected 3 non-empty segments, got %d: %v", len(cap.lines), cap.lines)
	}
}

func TestSplitLineSink_PropagatesWriteError(t *testing.T) {
	cfg := pipeline.DefaultSplitLineConfig()
	errSink := &errorSink{err: errors.New("write failed")}
	s, _ := pipeline.NewSplitLineSink(cfg, errSink)
	err := s.Write(context.Background(), "x,y,z")
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

func TestSplitLineSink_Close(t *testing.T) {
	cap := &captureSink{name: "cap"}
	s, _ := pipeline.NewSplitLineSink(pipeline.DefaultSplitLineConfig(), cap)
	if err := s.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !cap.closed {
		t.Fatal("expected inner sink to be closed")
	}
}
