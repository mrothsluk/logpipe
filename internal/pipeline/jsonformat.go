package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yourorg/logpipe/internal/sink"
)

// JSONFormatConfig controls how log lines are wrapped in JSON.
type JSONFormatConfig struct {
	// MessageField is the JSON key used for the raw log line. Defaults to "message".
	MessageField string
	// AddTimestamp adds a "ts" field with the current UTC time in RFC3339Nano.
	AddTimestamp bool
	// ExtraFields are static key/value pairs appended to every JSON object.
	ExtraFields map[string]string
}

// DefaultJSONFormatConfig returns a JSONFormatConfig with sensible defaults.
func DefaultJSONFormatConfig() JSONFormatConfig {
	return JSONFormatConfig{
		MessageField: "message",
		AddTimestamp: true,
	}
}

type jsonFormatSink struct {
	cfg   JSONFormatConfig
	inner sink.Sink
}

// NewJSONFormatSink wraps inner so that each line is emitted as a JSON object.
// Returns an error if inner is nil or MessageField is empty.
func NewJSONFormatSink(cfg JSONFormatConfig, inner sink.Sink) (sink.Sink, error) {
	if inner == nil {
		return nil, fmt.Errorf("jsonformat: inner sink must not be nil")
	}
	if cfg.MessageField == "" {
		cfg.MessageField = "message"
	}
	return &jsonFormatSink{cfg: cfg, inner: inner}, nil
}

func (j *jsonFormatSink) Name() string {
	return "jsonformat(" + j.inner.Name() + ")"
}

func (j *jsonFormatSink) Write(ctx context.Context, line string) error {
	m := make(map[string]any, 2+len(j.cfg.ExtraFields))
	m[j.cfg.MessageField] = line
	if j.cfg.AddTimestamp {
		m["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	for k, v := range j.cfg.ExtraFields {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("jsonformat: marshal: %w", err)
	}
	return j.inner.Write(ctx, string(b))
}

func (j *jsonFormatSink) Close() error {
	return j.inner.Close()
}
