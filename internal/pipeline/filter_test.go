package pipeline_test

import (
	"context"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
)

// captureFilter is a minimal sink that records written lines.
type captureFilter struct {
	lines []string
}

func (c *captureFilter) Name() string { return "capture" }
func (c *captureFilter) Write(_ context.Context, line string) error {
	c.lines = append(c.lines, line)
	return nil
}
func (c *captureFilter) Close() error { return nil }

func TestFilter_NoPatterns_PassesAll(t *testing.T) {
	cap := &captureFilter{}
	f, err := pipeline.NewFilter(pipeline.FilterConfig{}, cap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := []string{"hello", "world", "foo"}
	for _, l := range lines {
		_ = f.Write(context.Background(), l)
	}
	if len(cap.lines) != 3 {
		t.Errorf("expected 3 lines, got %d", len(cap.lines))
	}
}

func TestFilter_IncludePattern_FiltersOut(t *testing.T) {
	cap := &captureFilter{}
	f, err := pipeline.NewFilter(pipeline.FilterConfig{IncludePattern: `^ERROR`}, cap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = f.Write(context.Background(), "ERROR: disk full")
	_ = f.Write(context.Background(), "INFO: all good")
	if len(cap.lines) != 1 || cap.lines[0] != "ERROR: disk full" {
		t.Errorf("expected only ERROR line, got %v", cap.lines)
	}
}

func TestFilter_ExcludePattern_DropsMatch(t *testing.T) {
	cap := &captureFilter{}
	f, err := pipeline.NewFilter(pipeline.FilterConfig{ExcludePattern: `DEBUG`}, cap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = f.Write(context.Background(), "DEBUG: verbose")
	_ = f.Write(context.Background(), "INFO: important")
	if len(cap.lines) != 1 || cap.lines[0] != "INFO: important" {
		t.Errorf("expected only INFO line, got %v", cap.lines)
	}
}

func TestFilter_IncludeAndExclude_ExcludeTakesPrecedence(t *testing.T) {
	cap := &captureFilter{}
	f, err := pipeline.NewFilter(pipeline.FilterConfig{
		IncludePattern: `ERROR`,
		ExcludePattern: `IGNORE`,
	}, cap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = f.Write(context.Background(), "ERROR: real")
	_ = f.Write(context.Background(), "ERROR: IGNORE this")
	_ = f.Write(context.Background(), "INFO: skipped")
	if len(cap.lines) != 1 || cap.lines[0] != "ERROR: real" {
		t.Errorf("expected one line, got %v", cap.lines)
	}
}

func TestFilter_InvalidIncludePattern_ReturnsError(t *testing.T) {
	_, err := pipeline.NewFilter(pipeline.FilterConfig{IncludePattern: `[invalid`}, &captureFilter{})
	if err == nil {
		t.Error("expected error for invalid include pattern")
	}
}

func TestFilter_InvalidExcludePattern_ReturnsError(t *testing.T) {
	_, err := pipeline.NewFilter(pipeline.FilterConfig{ExcludePattern: `[invalid`}, &captureFilter{})
	if err == nil {
		t.Error("expected error for invalid exclude pattern")
	}
}
