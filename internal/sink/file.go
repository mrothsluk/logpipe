package sink

import (
	"fmt"
	"os"
	"sync"
)

// FileSink writes log lines to a file on disk.
type FileSink struct {
	mu   sync.Mutex
	name string
	path string
	f    *os.File
}

// NewFileSink opens (or creates) the file at path for appending and returns a FileSink.
func NewFileSink(name, path string) (*FileSink, error) {
	if path == "" {
		return nil, fmt.Errorf("file sink %q: path must not be empty", name)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("file sink %q: open %s: %w", name, path, err)
	}
	return &FileSink{name: name, path: path, f: f}, nil
}

// Name returns the configured sink name.
func (s *FileSink) Name() string { return s.name }

// Path returns the file path this sink writes to.
func (s *FileSink) Path() string { return s.path }

// Write appends the log line (plus newline) to the file.
func (s *FileSink) Write(line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return fmt.Errorf("file sink %q: already closed", s.name)
	}
	_, err := fmt.Fprintln(s.f, line)
	if err != nil {
		return fmt.Errorf("file sink %q: write: %w", s.name, err)
	}
	return nil
}

// Rotate closes the current file, reopens it (truncating), and continues writing.
// This is useful when an external log-rotation tool has moved the old file away
// and a fresh file should be started at the same path.
func (s *FileSink) Rotate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			return fmt.Errorf("file sink %q: rotate close: %w", s.name, err)
		}
		s.f = nil
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("file sink %q: rotate open %s: %w", s.name, s.path, err)
	}
	s.f = f
	return nil
}

// Close flushes and closes the underlying file.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
