package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/yourorg/logpipe/internal/sink"
)

// BatchConfig controls how lines are batched before flushing to a sink.
type BatchConfig struct {
	// MaxSize is the maximum number of lines in a batch before flushing.
	MaxSize int
	// MaxDelay is the maximum time to wait before flushing an incomplete batch.
	MaxDelay time.Duration
}

// DefaultBatchConfig returns sensible defaults.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxSize:  100,
		MaxDelay: 500 * time.Millisecond,
	}
}

// Batcher accumulates lines and flushes them to a sink either when the batch
// reaches MaxSize or when MaxDelay elapses since the first line in the batch.
type Batcher struct {
	cfg  BatchConfig
	sink sink.Sink
	mu   sync.Mutex
	buf  []string
}

// NewBatcher creates a Batcher wrapping the provided sink.
func NewBatcher(cfg BatchConfig, s sink.Sink) (*Batcher, error) {
	if s == nil {
		return nil, errorf("batcher: sink must not be nil")
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = DefaultBatchConfig().MaxSize
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultBatchConfig().MaxDelay
	}
	return &Batcher{cfg: cfg, sink: s}, nil
}

// Run reads lines from in, batches them, and flushes to the sink. It blocks
// until ctx is cancelled or in is closed.
func (b *Batcher) Run(ctx context.Context, in <-chan string) {
	ticker := time.NewTicker(b.cfg.MaxDelay)
	defer ticker.Stop()

	for {
		select {
		case line, ok := <-in:
			if !ok {
				b.flush(ctx)
				return
			}
			b.mu.Lock()
			b.buf = append(b.buf, line)
			ready := len(b.buf) >= b.cfg.MaxSize
			b.mu.Unlock()
			if ready {
				b.flush(ctx)
			}
		case <-ticker.C:
			b.flush(ctx)
		case <-ctx.Done():
			b.flush(ctx)
			return
		}
	}
}

// flush writes all buffered lines to the sink, one per call to Write.
func (b *Batcher) flush(ctx context.Context) {
	b.mu.Lock()
	lines := b.buf
	b.buf = nil
	b.mu.Unlock()

	for _, l := range lines {
		_ = b.sink.Write(ctx, l)
	}
}
