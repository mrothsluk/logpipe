package pipeline

import (
	"sync/atomic"
)

// Metrics holds counters for pipeline activity.
type Metrics struct {
	LinesIn      atomic.Int64
	LinesOut     atomic.Int64
	LinesDropped atomic.Int64
	LinesFiltered atomic.Int64
	Errors       atomic.Int64
}

// Snapshot returns a point-in-time copy of the metrics as plain int64 values.
type MetricsSnapshot struct {
	LinesIn       int64
	LinesOut      int64
	LinesDropped  int64
	LinesFiltered int64
	Errors        int64
}

// Snapshot captures current metric values atomically.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		LinesIn:       m.LinesIn.Load(),
		LinesOut:      m.LinesOut.Load(),
		LinesDropped:  m.LinesDropped.Load(),
		LinesFiltered: m.LinesFiltered.Load(),
		Errors:        m.Errors.Load(),
	}
}

// Reset zeroes all counters.
func (m *Metrics) Reset() {
	m.LinesIn.Store(0)
	m.LinesOut.Store(0)
	m.LinesDropped.Store(0)
	m.LinesFiltered.Store(0)
	m.Errors.Store(0)
}

// InstrumentedQueue wraps a BoundedQueue and records drop/enqueue metrics.
type InstrumentedQueue struct {
	queue   *BoundedQueue
	metrics *Metrics
}

// NewInstrumentedQueue creates an InstrumentedQueue backed by the given BoundedQueue.
func NewInstrumentedQueue(q *BoundedQueue, m *Metrics) *InstrumentedQueue {
	if m == nil {
		m = &Metrics{}
	}
	return &InstrumentedQueue{queue: q, metrics: m}
}

// Enqueue records the attempt and delegates to the underlying queue.
// Returns true if the line was accepted, false if dropped.
func (iq *InstrumentedQueue) Enqueue(line string) bool {
	iq.metrics.LinesIn.Add(1)
	if iq.queue.Enqueue(line) {
		return true
	}
	iq.metrics.LinesDropped.Add(1)
	return false
}

// Queue returns the underlying BoundedQueue for draining.
func (iq *InstrumentedQueue) Queue() *BoundedQueue {
	return iq.queue
}

// Metrics returns the shared Metrics instance.
func (iq *InstrumentedQueue) Metrics() *Metrics {
	return iq.metrics
}
