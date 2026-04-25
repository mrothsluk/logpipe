package tail

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestTailer_EmitsNewLines(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "logpipe-*.log")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()

	// Write some existing content that should be skipped.
	_, _ = f.WriteString("old line\n")

	out := make(chan Line, 10)
	tr := New(f.Name(), out, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Run(ctx)
	}()

	// Give the tailer time to seek to end.
	time.Sleep(100 * time.Millisecond)

	// Write new lines that should be picked up.
	_, _ = f.WriteString("hello world\n")
	_, _ = f.WriteString("second line\n")

	got := map[string]bool{}
	timeout := time.After(1 * time.Second)
collect:
	for len(got) < 2 {
		select {
		case l := <-out:
			got[l.Text] = true
		case <-timeout:
			t.Fatalf("timed out waiting for lines, got: %v", got)
		}
	}
	cancel()
	_ = <-errCh

	if !got["hello world"] {
		t.Errorf("expected 'hello world', got: %v", got)
	}
	if !got["second line"] {
		t.Errorf("expected 'second line', got: %v", got)
	}
	goto done
collect:
done:
}

func TestTailer_FileNotFound(t *testing.T) {
	out := make(chan Line, 1)
	tr := New("/nonexistent/path/file.log", out, 50*time.Millisecond)
	err := tr.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestReadNewLines_SplitsCorrectly(t *testing.T) {
	buf := []byte("partial")
	f, err := os.CreateTemp(t.TempDir(), "rnl-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, _ = f.WriteString(" line\nfull line\n")
	_, _ = f.Seek(0, 0)

	lines, err := readNewLines(f, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %v", len(lines), lines)
	}
	if lines[0] != "partial line" {
		t.Errorf("line[0] = %q, want %q", lines[0], "partial line")
	}
	if lines[1] != "full line" {
		t.Errorf("line[1] = %q, want %q", lines[1], "full line")
	}
}
