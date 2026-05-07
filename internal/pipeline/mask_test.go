package pipeline_test

import (
	"context"
	"regexp"
	"sync"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
)

func TestMaskSink_NilInner(t *testing.T) {
	_, err := pipeline.NewMaskSink(nil, pipeline.MaskConfig{
		Patterns: map[*regexp.Regexp]string{
			regexp.MustCompile(`secret`): "***",
		},
	})
	if err == nil {
		t.Fatal("expected error for nil inner sink")
	}
}

func TestMaskSink_NoPatterns(t *testing.T) {
	dummy := &captureSink{}
	_, err := pipeline.NewMaskSink(dummy, pipeline.MaskConfig{})
	if err == nil {
		t.Fatal("expected error for empty patterns")
	}
}

func TestMaskSink_Name(t *testing.T) {
	dummy := &captureSink{name: "stdout"}
	m, err := pipeline.NewMaskSink(dummy, pipeline.MaskConfig{
		Patterns: map[*regexp.Regexp]string{
			regexp.MustCompile(`x`): "y",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := m.Name(); got != "mask:stdout" {
		t.Errorf("Name() = %q, want %q", got, "mask:stdout")
	}
}

func TestMaskSink_RedactsPattern(t *testing.T) {
	dummy := &captureSink{name: "test"}
	m, err := pipeline.NewMaskSink(dummy, pipeline.MaskConfig{
		Patterns: map[*regexp.Regexp]string{
			regexp.MustCompile(`password=\S+`): "password=[REDACTED]",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := m.Write(ctx, "user login password=s3cr3t host=localhost"); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if len(dummy.lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(dummy.lines))
	}
	want := "user login password=[REDACTED] host=localhost"
	if dummy.lines[0] != want {
		t.Errorf("got %q, want %q", dummy.lines[0], want)
	}
}

func TestMaskSink_MultiplePatterns(t *testing.T) {
	dummy := &captureSink{name: "test"}
	m, err := pipeline.NewMaskSink(dummy, pipeline.MaskConfig{
		Patterns: map[*regexp.Regexp]string{
			regexp.MustCompile(`\d{4}-\d{4}-\d{4}-\d{4}`): "[CARD]",
			regexp.MustCompile(`token=\S+`):                "token=[REDACTED]",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	_ = m.Write(ctx, "charge 1234-5678-9012-3456 token=abc123")

	if len(dummy.lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(dummy.lines))
	}
	got := dummy.lines[0]
	if regexp.MustCompile(`\d{4}-\d{4}-\d{4}-\d{4}`).MatchString(got) {
		t.Errorf("card number not masked in: %q", got)
	}
	if regexp.MustCompile(`token=abc123`).MatchString(got) {
		t.Errorf("token not masked in: %q", got)
	}
}

func TestMaskSink_Close(t *testing.T) {
	dummy := &captureSink{name: "test"}
	m, _ := pipeline.NewMaskSink(dummy, pipeline.MaskConfig{
		Patterns: map[*regexp.Regexp]string{
			regexp.MustCompile(`x`): "y",
		},
	})
	if err := m.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
	if !dummy.closed {
		t.Error("expected inner sink to be closed")
	}
}

func TestMaskSink_ConcurrentSafe(t *testing.T) {
	dummy := &captureSink{name: "test"}
	m, _ := pipeline.NewMaskSink(dummy, pipeline.MaskConfig{
		Patterns: map[*regexp.Regexp]string{
			regexp.MustCompile(`secret=\S+`): "secret=[REDACTED]",
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Write(context.Background(), "key secret=topsecret")
		}()
	}
	wg.Wait()
}
