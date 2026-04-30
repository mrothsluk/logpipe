package pipeline

import (
	"context"
	"testing"
)

func TestSampler_InvalidRate_Zero(t *testing.T) {
	_, err := NewSampler(SamplingConfig{Rate: 0}, 42)
	if err == nil {
		t.Fatal("expected error for rate=0")
	}
}

func TestSampler_InvalidRate_Negative(t *testing.T) {
	_, err := NewSampler(SamplingConfig{Rate: -0.5}, 42)
	if err == nil {
		t.Fatal("expected error for negative rate")
	}
}

func TestSampler_InvalidRate_OverOne(t *testing.T) {
	_, err := NewSampler(SamplingConfig{Rate: 1.1}, 42)
	if err == nil {
		t.Fatal("expected error for rate > 1.0")
	}
}

func TestSampler_RateOne_PassesAll(t *testing.T) {
	s, err := NewSampler(DefaultSamplingConfig(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := make(chan string, 10)
	out := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lines := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, l := range lines {
		in <- l
	}
	close(in)

	s.Filter(ctx, in, out)
	close(out)

	var got []string
	for l := range out {
		got = append(got, l)
	}
	if len(got) != len(lines) {
		t.Fatalf("rate=1.0: expected %d lines, got %d", len(lines), len(got))
	}
}

func TestSampler_RateHalf_DropsRoughlyHalf(t *testing.T) {
	s, err := NewSampler(SamplingConfig{Rate: 0.5}, 12345)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const total = 10000
	in := make(chan string, total)
	out := make(chan string, total)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < total; i++ {
		in <- "line"
	}
	close(in)

	s.Filter(ctx, in, out)
	close(out)

	var kept int
	for range out {
		kept++
	}

	lo, hi := total*35/100, total*65/100
	if kept < lo || kept > hi {
		t.Fatalf("rate=0.5: kept %d/%d lines, expected roughly 50%% (±15%%)", kept, total)
	}
}

func TestSampler_ContextCancel_StopsEarly(t *testing.T) {
	s, err := NewSampler(DefaultSamplingConfig(), 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := make(chan string) // unbuffered, never written
	out := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	s.Filter(ctx, in, out) // should return without blocking
}

func TestDefaultSamplingConfig(t *testing.T) {
	cfg := DefaultSamplingConfig()
	if cfg.Rate != 1.0 {
		t.Fatalf("expected default rate 1.0, got %v", cfg.Rate)
	}
}
