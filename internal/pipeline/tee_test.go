package pipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
)

// recordSink captures written lines for assertion in tests.
type recordSink struct {
	mu     sync.Mutex
	name   string
	lines  []string
	wErr   error
	closed bool
}

func (r *recordSink) Name() string { return r.name }
func (r *recordSink) Write(_ context.Context, line string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wErr != nil {
		return r.wErr
	}
	r.lines = append(r.lines, line)
	return nil
}
func (r *recordSink) Close() error { r.closed = true; return nil }

func TestTeeSink_NilPrimary(t *testing.T) {
	_, err := pipeline.NewTeeSink(nil)
	if err == nil {
		t.Fatal("expected error for nil primary")
	}
}

func TestTeeSink_Name(t *testing.T) {
	primary := &recordSink{name: "stdout"}
	tee, _ := pipeline.NewTeeSink(primary)
	if tee.Name() != "tee(stdout)" {
		t.Fatalf("unexpected name: %s", tee.Name())
	}
}

func TestTeeSink_WritesToAllSinks(t *testing.T) {
	primary := &recordSink{name: "primary"}
	sec1 := &recordSink{name: "sec1"}
	sec2 := &recordSink{name: "sec2"}

	tee, _ := pipeline.NewTeeSink(primary, sec1, sec2)
	ctx := context.Background()

	if err := tee.Write(ctx, "hello"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, s := range []*recordSink{primary, sec1, sec2} {
		if len(s.lines) != 1 || s.lines[0] != "hello" {
			t.Errorf("%s: expected [hello], got %v", s.name, s.lines)
		}
	}
}

func TestTeeSink_SecondaryErrorDoesNotBlockPrimary(t *testing.T) {
	primary := &recordSink{name: "primary"}
	failing := &recordSink{name: "failing", wErr: errors.New("boom")}

	tee, _ := pipeline.NewTeeSink(primary, failing)
	if err := tee.Write(context.Background(), "line"); err != nil {
		t.Fatalf("primary should not fail: %v", err)
	}
	if len(primary.lines) != 1 {
		t.Errorf("primary should have received line")
	}
}

func TestTeeSink_PrimaryErrorIsReturned(t *testing.T) {
	primary := &recordSink{name: "primary", wErr: errors.New("primary down")}
	tee, _ := pipeline.NewTeeSink(primary)
	if err := tee.Write(context.Background(), "line"); err == nil {
		t.Fatal("expected error from primary")
	}
}

func TestTeeSink_CloseAll(t *testing.T) {
	primary := &recordSink{name: "primary"}
	sec := &recordSink{name: "sec"}
	tee, _ := pipeline.NewTeeSink(primary, sec)
	if err := tee.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !primary.closed || !sec.closed {
		t.Error("expected all sinks to be closed")
	}
}
