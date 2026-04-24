package config

import (
	"os"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "logpipe-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	f.Close()
	return f.Name()
}

func TestLoad_Valid(t *testing.T) {
	raw := `
inputs:
  - path: /var/log/app.log
    tags:
      env: prod
sinks:
  - name: stdout
    type: stdout
buffer:
  capacity: 1024
  workers: 4
`
	path := writeTempConfig(t, raw)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Inputs) != 1 {
		t.Errorf("expected 1 input, got %d", len(cfg.Inputs))
	}
	if cfg.Inputs[0].Path != "/var/log/app.log" {
		t.Errorf("unexpected input path: %s", cfg.Inputs[0].Path)
	}
	if cfg.Buffer.Capacity != 1024 {
		t.Errorf("expected capacity 1024, got %d", cfg.Buffer.Capacity)
	}
	if cfg.Buffer.Workers != 4 {
		t.Errorf("expected 4 workers, got %d", cfg.Buffer.Workers)
	}
}

func TestLoad_DefaultsApplied(t *testing.T) {
	raw := `
inputs:
  - path: /tmp/test.log
sinks:
  - name: out
    type: stdout
`
	path := writeTempConfig(t, raw)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Buffer.Capacity != 4096 {
		t.Errorf("expected default capacity 4096, got %d", cfg.Buffer.Capacity)
	}
	if cfg.Buffer.Workers != 2 {
		t.Errorf("expected default workers 2, got %d", cfg.Buffer.Workers)
	}
}

func TestLoad_MissingInputs(t *testing.T) {
	raw := `
sinks:
  - name: out
    type: stdout
`
	path := writeTempConfig(t, raw)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing inputs")
	}
}

func TestLoad_MissingSinks(t *testing.T) {
	raw := `
inputs:
  - path: /tmp/test.log
`
	path := writeTempConfig(t, raw)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing sinks")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
