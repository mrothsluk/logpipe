package pipeline

import (
	"context"
	"math/rand"
	"sync"
)

// SamplingConfig holds configuration for the sampler.
type SamplingConfig struct {
	// Rate is the fraction of lines to keep, in the range (0.0, 1.0].
	// A rate of 1.0 passes every line; 0.5 passes roughly half.
	Rate float64
}

// DefaultSamplingConfig returns a SamplingConfig that keeps all lines.
func DefaultSamplingConfig() SamplingConfig {
	return SamplingConfig{Rate: 1.0}
}

// Sampler probabilistically drops log lines based on a configured rate.
type Sampler struct {
	cfg  SamplingConfig
	rng  *rand.Rand
	mu   sync.Mutex
}

// NewSampler creates a Sampler with the given config and a seeded RNG.
func NewSampler(cfg SamplingConfig, seed int64) (*Sampler, error) {
	if cfg.Rate <= 0 || cfg.Rate > 1.0 {
		return nil, errorf("sampling rate must be in (0.0, 1.0], got %v", cfg.Rate)
	}
	return &Sampler{
		cfg: cfg,
		rng: rand.New(rand.NewSource(seed)), //nolint:gosec
	}, nil
}

// keep returns true if the line should be forwarded.
func (s *Sampler) keep() bool {
	if s.cfg.Rate >= 1.0 {
		return true
	}
	s.mu.Lock()
	v := s.rng.Float64()
	s.mu.Unlock()
	return v < s.cfg.Rate
}

// Filter reads from in, probabilistically forwards lines to out, and returns
// when ctx is cancelled or in is closed.
func (s *Sampler) Filter(ctx context.Context, in <-chan string, out chan<- string) {
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-in:
			if !ok {
				return
			}
			if s.keep() {
				select {
				case out <- line:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// errorf is a local helper to avoid importing fmt at package level.
func errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
