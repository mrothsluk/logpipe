package pipeline

import (
	"context"
	"sync"
	"time"
)

// BackpressureConfig holds tuning parameters for the bounded queue.
type BackpressureConfig struct {
	Capacity    int           // max number of lines buffered
	WriteTimeout time.Duration // how long a producer waits before dropping
}

// DefaultBackpressureConfig returns sensible defaults.
func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		Capacity:    4096,
		WriteTimeout: 200 * time.Millisecond,
	}
}

// BoundedQueue is a channel-backed queue that applies backpressure to
// producers when consumers fall behind.
type BoundedQueue struct {
	cfg  BackpressureConfig
	ch   chan string
	wg   sync.WaitGroup
	once sync.Once
}

// NewBoundedQueue creates a BoundedQueue and starts draining into handler.
func NewBoundedQueue(cfg BackpressureConfig, handler func(line string)) *BoundedQueue {
	bq := &BoundedQueue{
		cfg: cfg,
		ch:  make(chan string, cfg.Capacity),
	}
	bq.wg.Add(1)
	go func() {
		defer bq.wg.Done()
		for line := range bq.ch {
			handler(line)
		}
	}()
	return bq
}

// Enqueue attempts to send line to the queue within WriteTimeout.
// Returns false if the queue is full and the timeout expires (line is dropped).
func (bq *BoundedQueue) Enqueue(ctx context.Context, line string) bool {
	select {
	case bq.ch <- line:
		return true
	case <-time.After(bq.cfg.WriteTimeout):
		return false
	case <-ctx.Done():
		return false
	}
}

// Close drains in-flight lines and shuts down the drain goroutine.
func (bq *BoundedQueue) Close() {
	bq.once.Do(func() {
		close(bq.ch)
		bq.wg.Wait()
	})
}

// Len returns the current number of items waiting in the queue.
func (bq *BoundedQueue) Len() int {
	return len(bq.ch)
}
