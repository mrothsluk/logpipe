package pipeline

import (
	"testing"
	"time"
)

func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestDedupFilter_FirstOccurrenceNotDuplicate(t *testing.T) {
	f := NewDedupFilter(DefaultDedupConfig())
	if f.IsDuplicate("hello world") {
		t.Fatal("expected first occurrence to not be a duplicate")
	}
}

func TestDedupFilter_SecondOccurrenceWithinWindowIsDuplicate(t *testing.T) {
	f := NewDedupFilter(DefaultDedupConfig())
	f.IsDuplicate("hello world")
	if !f.IsDuplicate("hello world") {
		t.Fatal("expected second occurrence within window to be a duplicate")
	}
}

func TestDedupFilter_OccurrenceAfterWindowNotDuplicate(t *testing.T) {
	base := time.Now()
	cfg := DedupConfig{Window: 1 * time.Second, MaxEntries: 100}
	f := NewDedupFilter(cfg)
	f.nowFunc = fixedClock(base)

	f.IsDuplicate("expiring line")

	// Advance clock past the window.
	f.nowFunc = fixedClock(base.Add(2 * time.Second))
	if f.IsDuplicate("expiring line") {
		t.Fatal("expected line to be fresh after window expiry")
	}
}

func TestDedupFilter_DifferentLinesNotDuplicates(t *testing.T) {
	f := NewDedupFilter(DefaultDedupConfig())
	f.IsDuplicate("line one")
	if f.IsDuplicate("line two") {
		t.Fatal("distinct lines should not be considered duplicates")
	}
}

func TestDedupFilter_EvictsWhenFull(t *testing.T) {
	base := time.Now()
	cfg := DedupConfig{Window: 10 * time.Second, MaxEntries: 3}
	f := NewDedupFilter(cfg)
	f.nowFunc = fixedClock(base)

	// Fill up to MaxEntries with lines that will expire.
	f.IsDuplicate("a")
	f.IsDuplicate("b")
	f.IsDuplicate("c")

	// Advance past window so existing entries are evictable.
	f.nowFunc = fixedClock(base.Add(20 * time.Second))

	// This should trigger eviction and succeed without panic.
	if f.IsDuplicate("d") {
		t.Fatal("new line after eviction should not be a duplicate")
	}
}

func TestDedupFilter_Len(t *testing.T) {
	f := NewDedupFilter(DefaultDedupConfig())
	if f.Len() != 0 {
		t.Fatalf("expected 0, got %d", f.Len())
	}
	f.IsDuplicate("x")
	f.IsDuplicate("y")
	if f.Len() != 2 {
		t.Fatalf("expected 2, got %d", f.Len())
	}
}
