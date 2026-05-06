package pipeline

import (
	"sync"
	"time"
)

// WindowedDedup is a sliding-window deduplication filter that tracks seen
// lines within a configurable time window and drops exact duplicates.
// Unlike DedupFilter which uses a fixed TTL per entry, WindowedDedup evicts
// all entries older than the window on each insertion, keeping memory bounded.
type WindowedDedup struct {
	mu     sync.Mutex
	window time.Duration
	seen   map[string]time.Time
	clock  func() time.Time
}

// NewWindowedDedup creates a WindowedDedup with the given sliding window
// duration. Panics if window is zero or negative.
func NewWindowedDedup(window time.Duration) *WindowedDedup {
	if window <= 0 {
		panic("pipeline: WindowedDedup window must be positive")
	}
	return &WindowedDedup{
		window: window,
		seen:   make(map[string]time.Time),
		clock:  time.Now,
	}
}

// IsDuplicate returns true if line was seen within the sliding window.
// It also evicts stale entries on each call to prevent unbounded growth.
func (w *WindowedDedup) IsDuplicate(line string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.clock()
	w.evict(now)

	if _, ok := w.seen[line]; ok {
		return true
	}
	w.seen[line] = now
	return false
}

// evict removes all entries older than the window. Must be called with mu held.
func (w *WindowedDedup) evict(now time.Time) {
	cutoff := now.Add(-w.window)
	for k, t := range w.seen {
		if t.Before(cutoff) {
			delete(w.seen, k)
		}
	}
}

// Len returns the number of currently tracked unique lines.
func (w *WindowedDedup) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.seen)
}

// Reset clears all tracked entries.
func (w *WindowedDedup) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seen = make(map[string]time.Time)
}
