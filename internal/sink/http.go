package sink

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"
)

// HTTPSink ships log lines to an HTTP endpoint via POST requests.
type HTTPSink struct {
	name    string
	endpoint string
	headers  map[string]string
	client   *http.Client
}

// HTTPSinkConfig holds configuration for an HTTPSink.
type HTTPSinkConfig struct {
	Name     string
	Endpoint string
	Headers  map[string]string
	Timeout  time.Duration
}

// NewHTTPSink creates a new HTTPSink from the provided config.
func NewHTTPSink(cfg HTTPSinkConfig) (*HTTPSink, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("http sink %q: endpoint must not be empty", cfg.Name)
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &HTTPSink{
		name:     cfg.Name,
		endpoint: cfg.Endpoint,
		headers:  cfg.Headers,
		client:   &http.Client{Timeout: timeout},
	}, nil
}

// Name returns the sink identifier.
func (s *HTTPSink) Name() string {
	return s.name
}

// Write sends a log line to the configured HTTP endpoint.
func (s *HTTPSink) Write(ctx context.Context, line string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewBufferString(line))
	if err != nil {
		return fmt.Errorf("http sink %q: create request: %w", s.name, err)
	}
	req.Header.Set("Content-Type", "text/plain")
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http sink %q: send request: %w", s.name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http sink %q: unexpected status %d", s.name, resp.StatusCode)
	}
	return nil
}

// Close is a no-op for HTTPSink but satisfies the Sink interface.
func (s *HTTPSink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
