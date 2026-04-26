package sink

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestFileSink_Name(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileSink("myfile", filepath.Join(dir, "out.log"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()
	if s.Name() != "myfile" {
		t.Errorf("expected name 'myfile', got %q", s.Name())
	}
}

func TestFileSink_EmptyPath(t *testing.T) {
	_, err := NewFileSink("bad", "")
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestFileSink_Write_Success(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.log")
	s, err := NewFileSink("test", path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	if err := s.Write("hello world"); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !strings.Contains(string(data), "hello world") {
		t.Errorf("expected 'hello world' in file, got %q", string(data))
	}
}

func TestFileSink_Close_Idempotent(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileSink("test", filepath.Join(dir, "out.log"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close should be a no-op: %v", err)
	}
}

func TestFileSink_Write_AfterClose(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileSink("test", filepath.Join(dir, "out.log"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s.Close()
	if err := s.Write("should fail"); err == nil {
		t.Fatal("expected error writing to closed sink")
	}
}

func TestFileSink_Write_ConcurrentSafe(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileSink("concurrent", filepath.Join(dir, "out.log"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s.Write("line")
		}(i)
	}
	wg.Wait()
}
