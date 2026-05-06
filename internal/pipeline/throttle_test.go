package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestThrottleSink_Name(t *testing.T) {
	s := &captureSink{name: "stdout"}
	th, err := NewThrottleSink(s, DefaultThrottleConfig())
	if err != nil {
		t.Fatal(err)
	}
	if th.Name() != "throttle(stdout)" {
		t.Fatalf("unexpected name: %s", th.Name())
	}
}

func TestThrottleSink_NilInner(t *testing.T) {
	_, err := NewThrottleSink(nil, DefaultThrottleConfig())
	if err == nil {
		t.Fatal("expected error for nil inner sink")
	}
}

func TestThrottleSink_ZeroInterval(t *testing.T) {
	s := &captureSink{name: "stdout"}
	_, err := NewThrottleSink(s, ThrottleConfig{Interval: 0})
	if err == nil {
		t.Fatal("expected error for zero interval")
	}
}

func TestThrottleSink_FirstLineForwarded(t *testing.T) {
	s := &captureSink{name: "stdout"}
	th, _ := NewThrottleSink(s, DefaultThrottleConfig())
	if err := th.Write(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if s.count != 1 {
		t.Fatalf("expected 1 write, got %d", s.count)
	}
}

func TestThrottleSink_SecondLineWithinIntervalSuppressed(t *testing.T) {
	s := &captureSink{name: "stdout"}
	th, _ := NewThrottleSink(s, ThrottleConfig{Interval: 10 * time.Second})
	_ = th.Write(context.Background(), "hello")
	_ = th.Write(context.Background(), "hello again")
	if s.count != 1 {
		t.Fatalf("expected 1 write (second suppressed), got %d", s.count)
	}
}

func TestThrottleSink_PerKeyThrottling(t *testing.T) {
	s := &captureSink{name: "stdout"}
	keyFn := func(line string) string { return line[:1] }
	th, _ := NewThrottleSink(s, ThrottleConfig{Interval: 10 * time.Second, KeyFunc: keyFn})
	_ = th.Write(context.Background(), "a-first")
	_ = th.Write(context.Background(), "b-first") // different key — must pass
	_ = th.Write(context.Background(), "a-second") // same key — suppressed
	if s.count != 2 {
		t.Fatalf("expected 2 writes, got %d", s.count)
	}
}

func TestThrottleSink_ConcurrentSafe(t *testing.T) {
	s := &captureSink{name: "stdout"}
	th, _ := NewThrottleSink(s, ThrottleConfig{Interval: time.Millisecond})
	var wg sync.WaitGroup
	const goroutines = 50
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = th.Write(context.Background(), "line")
		}()
	}
	wg.Wait()
	// Just ensure no race — count can be anything
	_ = atomic.LoadInt64(nil) // keep import if needed
}
