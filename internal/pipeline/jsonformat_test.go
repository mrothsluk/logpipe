package pipeline_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/yourorg/logpipe/internal/pipeline"
	"github.com/yourorg/logpipe/internal/sink"
)

// captureSink records the last written line.
type captureSink struct {
	last   string
	closed bool
}

func (c *captureSink) Name() string                              { return "capture" }
func (c *captureSink) Write(_ context.Context, line string) error { c.last = line; return nil }
func (c *captureSink) Close() error                              { c.closed = true; return nil }

func TestJSONFormatSink_NilInner(t *testing.T) {
	_, err := pipeline.NewJSONFormatSink(pipeline.DefaultJSONFormatConfig(), nil)
	if err == nil {
		t.Fatal("expected error for nil inner sink")
	}
}

func TestJSONFormatSink_Name(t *testing.T) {
	cap := &captureSink{}
	s, _ := pipeline.NewJSONFormatSink(pipeline.DefaultJSONFormatConfig(), cap)
	if !strings.HasPrefix(s.Name(), "jsonformat(") {
		t.Fatalf("unexpected name: %s", s.Name())
	}
}

func TestJSONFormatSink_ContainsMessageField(t *testing.T) {
	cap := &captureSink{}
	cfg := pipeline.DefaultJSONFormatConfig()
	cfg.AddTimestamp = false
	s, _ := pipeline.NewJSONFormatSink(cfg, cap)

	if err := s.Write(context.Background(), "hello world"); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(cap.last), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v — got: %s", err, cap.last)
	}
	if m["message"] != "hello world" {
		t.Fatalf("expected message field, got %v", m)
	}
}

func TestJSONFormatSink_AddTimestamp(t *testing.T) {
	cap := &captureSink{}
	cfg := pipeline.DefaultJSONFormatConfig()
	cfg.AddTimestamp = true
	s, _ := pipeline.NewJSONFormatSink(cfg, cap)

	s.Write(context.Background(), "tick") //nolint:errcheck
	var m map[string]any
	json.Unmarshal([]byte(cap.last), &m) //nolint:errcheck
	if _, ok := m["ts"]; !ok {
		t.Fatalf("expected ts field in output: %s", cap.last)
	}
}

func TestJSONFormatSink_ExtraFields(t *testing.T) {
	cap := &captureSink{}
	cfg := pipeline.DefaultJSONFormatConfig()
	cfg.AddTimestamp = false
	cfg.ExtraFields = map[string]string{"env": "prod", "app": "logpipe"}
	s, _ := pipeline.NewJSONFormatSink(cfg, cap)

	s.Write(context.Background(), "line") //nolint:errcheck
	var m map[string]any
	json.Unmarshal([]byte(cap.last), &m) //nolint:errcheck
	if m["env"] != "prod" || m["app"] != "logpipe" {
		t.Fatalf("extra fields missing: %v", m)
	}
}

func TestJSONFormatSink_CustomMessageField(t *testing.T) {
	cap := &captureSink{}
	cfg := pipeline.DefaultJSONFormatConfig()
	cfg.MessageField = "log"
	cfg.AddTimestamp = false
	s, _ := pipeline.NewJSONFormatSink(cfg, cap)

	s.Write(context.Background(), "custom") //nolint:errcheck
	var m map[string]any
	json.Unmarshal([]byte(cap.last), &m) //nolint:errcheck
	if m["log"] != "custom" {
		t.Fatalf("expected 'log' field, got %v", m)
	}
}

func TestJSONFormatSink_Close(t *testing.T) {
	cap := &captureSink{}
	s, _ := pipeline.NewJSONFormatSink(pipeline.DefaultJSONFormatConfig(), cap)
	if err := s.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !cap.closed {
		t.Fatal("expected inner sink to be closed")
	}
}

// Ensure jsonFormatSink satisfies sink.Sink at compile time.
var _ sink.Sink = (*struct{ sink.Sink })(nil)
