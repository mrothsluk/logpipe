package tail

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

// Line represents a single log line read from a file.
type Line struct {
	Source string
	Text   string
	Time   time.Time
}

// Tailer tails a single file and emits lines to a channel.
type Tailer struct {
	path    string
	out     chan<- Line
	pollInterval time.Duration
}

// New creates a new Tailer for the given file path.
func New(path string, out chan<- Line, pollInterval time.Duration) *Tailer {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	return &Tailer{
		path:         path,
		out:          out,
		pollInterval: pollInterval,
	}
}

// Run opens the file, seeks to the end, and begins tailing.
// It blocks until ctx is cancelled.
func (t *Tailer) Run(ctx context.Context) error {
	f, err := os.Open(t.path)
	if err != nil {
		return fmt.Errorf("tail: open %q: %w", t.path, err)
	}
	defer f.Close()

	// Seek to end so we only ship new lines.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("tail: seek %q: %w", t.path, err)
	}

	buf := make([]byte, 0, 4096)
	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			lines, err := readNewLines(f, &buf)
			if err != nil {
				return fmt.Errorf("tail: read %q: %w", t.path, err)
			}
			for _, l := range lines {
				select {
				case t.out <- Line{Source: t.path, Text: l, Time: time.Now()}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

// readNewLines reads available bytes from f, splits on newlines,
// and returns complete lines. Partial lines are retained in buf.
func readNewLines(f *os.File, buf *[]byte) ([]string, error) {
	tmp := make([]byte, 4096)
	n, err := f.Read(tmp)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	*buf = append(*buf, tmp[:n]...)

	var lines []string
	start := 0
	for i, b := range *buf {
		if b == '\n' {
			line := string((*buf)[start:i])
			if line != "" {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	*buf = (*buf)[start:]
	return lines, nil
}
