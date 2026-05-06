package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ---- helpers ----------------------------------------------------------------

type captureMultilineSink struct {
	lines []string
}

func (c *captureMultilineSink) Name() string { return "capture" }
func (c *captureMultilineSink) Write(_ context.Context, line string) error {
	c.lines = append(c.lines, line)
	return nil
}
func (c *captureMultilineSink) Close() error { return nil }

func runFilter(t *testing.T, m *MultilineAggregator, inputs []string) []string {
	t.Helper()
	in := make(chan string, len(inputs))
	out := make(chan string, len(inputs)*2)
	for _, l := range inputs {
		in <- l
	}
	close(in)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m.Filter(ctx, in, out)
	close(out)
	var got []string
	for l := range out {
		got = append(got, l)
	}
	return got
}

// ---- tests ------------------------------------------------------------------

func TestMultilineAggregator_NilInner(t *testing.T) {
	_, err := NewMultilineAggregator(DefaultMultilineConfig(), nil)
	if err == nil {
		t.Fatal("expected error for nil inner sink")
	}
}

func TestMultilineAggregator_Name(t *testing.T) {
	cap := &captureMultilineSink{}
	m, _ := NewMultilineAggregator(DefaultMultilineConfig(), cap)
	if !strings.Contains(m.Name(), "multiline") {
		t.Errorf("unexpected name: %s", m.Name())
	}
}

func TestMultilineAggregator_Passthrough_EmptyPattern(t *testing.T) {
	cap := &captureMultilineSink{}
	cfg := DefaultMultilineConfig()
	cfg.StartPattern = ""
	m, _ := NewMultilineAggregator(cfg, cap)

	inputs := []string{"line1", "line2", "line3"}
	got := runFilter(t, m, inputs)
	if len(got) != 3 {
		t.Fatalf("expected 3 events, got %d: %v", len(got), got)
	}
}

func TestMultilineAggregator_FoldsContinuationLines(t *testing.T) {
	cap := &captureMultilineSink{}
	cfg := DefaultMultilineConfig()
	cfg.StartPattern = "2024-"
	m, _ := NewMultilineAggregator(cfg, cap)

	inputs := []string{
		"2024-01-01 ERROR something",
		"  at foo.go:10",
		"  at bar.go:20",
		"2024-01-01 INFO done",
	}
	got := runFilter(t, m, inputs)
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "at foo.go:10") {
		t.Errorf("first event missing continuation line: %s", got[0])
	}
}

func TestMultilineAggregator_MaxLines_ForcesFlush(t *testing.T) {
	cap := &captureMultilineSink{}
	cfg := MultilineConfig{
		StartPattern: "START",
		FlushTimeout: 5 * time.Second,
		MaxLines:     3,
	}
	m, _ := NewMultilineAggregator(cfg, cap)

	// 4 continuation lines after START — should produce two events (3+1)
	inputs := []string{"START", "c1", "c2", "c3", "c4"}
	got := runFilter(t, m, inputs)
	if len(got) < 2 {
		t.Fatalf("expected at least 2 events due to MaxLines flush, got %d: %v", len(got), got)
	}
}

func TestMultilineAggregator_DefaultsApplied(t *testing.T) {
	cap := &captureMultilineSink{}
	cfg := MultilineConfig{MaxLines: -1, FlushTimeout: -1}
	m, err := NewMultilineAggregator(cfg, cap)
	if err != nil {
		t.Fatal(err)
	}
	if m.cfg.MaxLines != DefaultMultilineConfig().MaxLines {
		t.Errorf("MaxLines default not applied")
	}
	if m.cfg.FlushTimeout != DefaultMultilineConfig().FlushTimeout {
		t.Errorf("FlushTimeout default not applied")
	}
}
