package pipeline

import (
	"testing"
	"time"
)

func TestWindowedDedup_PanicOnZeroWindow(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero window")
		}
	}()
	NewWindowedDedup(0)
}

func TestWindowedDedup_FirstOccurrenceNotDuplicate(t *testing.T) {
	wd := NewWindowedDedup(time.Minute)
	if wd.IsDuplicate("hello") {
		t.Error("first occurrence should not be a duplicate")
	}
}

func TestWindowedDedup_SecondOccurrenceWithinWindowIsDuplicate(t *testing.T) {
	wd := NewWindowedDedup(time.Minute)
	wd.IsDuplicate("hello")
	if !wd.IsDuplicate("hello") {
		t.Error("second occurrence within window should be a duplicate")
	}
}

func TestWindowedDedup_OccurrenceAfterWindowNotDuplicate(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	wd := NewWindowedDedup(time.Minute)
	wd.clock = func() time.Time { return now }

	wd.IsDuplicate("hello")

	// Advance clock beyond window.
	wd.clock = func() time.Time { return now.Add(2 * time.Minute) }

	if wd.IsDuplicate("hello") {
		t.Error("occurrence after window expiry should not be a duplicate")
	}
}

func TestWindowedDedup_DifferentLinesNotDuplicates(t *testing.T) {
	wd := NewWindowedDedup(time.Minute)
	wd.IsDuplicate("line-a")
	if wd.IsDuplicate("line-b") {
		t.Error("different lines should not be duplicates of each other")
	}
}

func TestWindowedDedup_EvictsStaleEntries(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	wd := NewWindowedDedup(time.Minute)
	wd.clock = func() time.Time { return now }

	for i := 0; i < 5; i++ {
		wd.IsDuplicate("line")
	}
	if wd.Len() != 1 {
		t.Fatalf("expected 1 tracked entry, got %d", wd.Len())
	}

	// Advance past window; next call should evict.
	wd.clock = func() time.Time { return now.Add(2 * time.Minute) }
	wd.IsDuplicate("other")

	if wd.Len() != 1 {
		t.Fatalf("expected stale entry evicted, got %d entries", wd.Len())
	}
}

func TestWindowedDedup_Reset(t *testing.T) {
	wd := NewWindowedDedup(time.Minute)
	wd.IsDuplicate("a")
	wd.IsDuplicate("b")
	wd.Reset()
	if wd.Len() != 0 {
		t.Fatalf("expected 0 entries after Reset, got %d", wd.Len())
	}
	if wd.IsDuplicate("a") {
		t.Error("entry should not be duplicate after Reset")
	}
}
