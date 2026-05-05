package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"logpipe/internal/sink"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

// CircuitBreakerConfig holds configuration for the circuit breaker.
type CircuitBreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	OpenDuration     time.Duration
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenDuration:     30 * time.Second,
	}
}

// CircuitBreakerSink wraps a sink with circuit breaker logic.
type CircuitBreakerSink struct {
	mu             sync.Mutex
	inner          sink.Sink
	cfg            CircuitBreakerConfig
	state          CircuitState
	failureCount   int
	successCount   int
	openedAt       time.Time
	now            func() time.Time
}

// ErrCircuitOpen is returned when the circuit is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// NewCircuitBreakerSink wraps the given sink with a circuit breaker.
func NewCircuitBreakerSink(inner sink.Sink, cfg CircuitBreakerConfig) *CircuitBreakerSink {
	return &CircuitBreakerSink{
		inner: inner,
		cfg:   cfg,
		state: StateClosed,
		now:   time.Now,
	}
}

func (c *CircuitBreakerSink) Name() string {
	return "circuit:" + c.inner.Name()
}

func (c *CircuitBreakerSink) Write(ctx context.Context, line string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case StateOpen:
		if c.now().Sub(c.openedAt) >= c.cfg.OpenDuration {
			c.state = StateHalfOpen
			c.successCount = 0
		} else {
			return ErrCircuitOpen
		}
	case StateClosed, StateHalfOpen:
		// proceed
	}

	err := c.inner.Write(ctx, line)
	if err != nil {
		c.failureCount++
		c.successCount = 0
		if c.failureCount >= c.cfg.FailureThreshold {
			c.state = StateOpen
			c.openedAt = c.now()
		}
		return err
	}

	c.successCount++
	if c.state == StateHalfOpen && c.successCount >= c.cfg.SuccessThreshold {
		c.state = StateClosed
		c.failureCount = 0
	}
	return nil
}

func (c *CircuitBreakerSink) Close() error {
	return c.inner.Close()
}

func (c *CircuitBreakerSink) State() CircuitState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}
