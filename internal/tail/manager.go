package tail

import (
	"context"
	"log"
	"sync"
	"time"
)

// Manager supervises multiple Tailers, one per input path.
type Manager struct {
	paths        []string
	out          chan Line
	pollInterval time.Duration
}

// NewManager creates a Manager that tails all provided paths.
// bufSize controls the buffer depth of the aggregated output channel;
// if <= 0 it defaults to 256. pollInterval controls how frequently each
// Tailer checks its file for new content.
func NewManager(paths []string, bufSize int, pollInterval time.Duration) *Manager {
	if bufSize <= 0 {
		bufSize = 256
	}
	return &Manager{
		paths:        paths,
		out:          make(chan Line, bufSize),
		pollInterval: pollInterval,
	}
}

// Lines returns the read-only channel of aggregated log lines.
func (m *Manager) Lines() <-chan Line {
	return m.out
}

// Run starts a Tailer goroutine for each path and blocks until ctx
// is cancelled. Tailer errors that occur before cancellation are logged
// but do not stop other tailers. When all tailers exit the output
// channel is closed.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, p := range m.paths {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := New(p, m.out, m.pollInterval)
			if err := t.Run(ctx); err != nil && ctx.Err() == nil {
				log.Printf("tailer error for %q: %v", p, err)
			}
		}()
	}
	wg.Wait()
	close(m.out)
}

// PathCount returns the number of paths this Manager is configured to tail.
func (m *Manager) PathCount() int {
	return len(m.paths)
}
