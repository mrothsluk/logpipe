package sink

import "context"

// Sink is the interface that all output destinations must implement.
type Sink interface {
	// Name returns a human-readable identifier for the sink.
	Name() string
	// Write sends a log line to the sink. Implementations must respect
	// context cancellation and apply backpressure by blocking when full.
	Write(ctx context.Context, line string) error
	// Close flushes any buffered data and releases resources.
	Close() error
}
