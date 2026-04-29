package pipeline

import (
	"sync"
	"time"
)

// DedupConfig holds configuration for the deduplication filter.
type DedupConfig struct {
	// Window is the duration during which duplicate lines are suppressed.
	Window time.Duration
	// MaxEntries is the maximum number of unique entries to track.
	MaxEntries int
}

// DefaultDedupConfig returns sensible dedup defaults.
func DefaultDedupConfig() DedupConfig {
	return DedupConfig{
		Window:     5 * time.Second,
		MaxEntries: 10_000,
	}
}

// DedupFilter suppresses duplicate log lines within a sliding time window.
type DedupFilter struct {
	mu      sync.Mutex
	seen    map[string]time.Time
	cfg     DedupConfig
	nowFunc func() time.Time
}

// NewDedupFilter creates a new DedupFilter with the given config.
func NewDedupFilter(cfg DedupConfig) *DedupFilter {
	return &DedupFilter{
		seen:    make(map[string]time.Time, cfg.MaxEntries),
		cfg:     cfg,
		nowFunc: time.Now,
	}
}

// IsDuplicate returns true if the line was seen within the configured window.
// It also records the line so future calls within the window return true.
func (d *DedupFilter) IsDuplicate(line string) bool {
	now := d.nowFunc()
	d.mu.Lock()
	defer d.mu.Unlock()

	// Evict expired entries to bound memory usage.
	if len(d.seen) >= d.cfg.MaxEntries {
		d.evictLocked(now)
	}

	if t, ok := d.seen[line]; ok && now.Sub(t) < d.cfg.Window {
		return true
	}

	d.seen[line] = now
	return false
}

// evictLocked removes all entries whose window has expired.
// Caller must hold d.mu.
func (d *DedupFilter) evictLocked(now time.Time) {
	for k, t := range d.seen {
		if now.Sub(t) >= d.cfg.Window {
			delete(d.seen, k)
		}
	}
}

// Len returns the number of currently tracked entries.
func (d *DedupFilter) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
