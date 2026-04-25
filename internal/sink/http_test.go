package sink

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPSink_Name(t *testing.T) {
	s, err := NewHTTPSink(HTTPSinkConfig{Name: "my-http", Endpoint: "http://example.com"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name() != "my-http" {
		t.Errorf("expected name %q, got %q", "my-http", s.Name())
	}
}

func TestHTTPSink_EmptyEndpoint(t *testing.T) {
	_, err := NewHTTPSink(HTTPSinkConfig{Name: "bad", Endpoint: ""})
	if err == nil {
		t.Fatal("expected error for empty endpoint, got nil")
	}
}

func TestHTTPSink_Write_Success(t *testing.T) {
	received := ""
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 512)
		n, _ := r.Body.Read(buf)
		received = string(buf[:n])
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	s, err := NewHTTPSink(HTTPSinkConfig{Name: "test", Endpoint: ts.URL})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	if err := s.Write(context.Background(), "hello world"); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if received != "hello world" {
		t.Errorf("expected body %q, got %q", "hello world", received)
	}
}

func TestHTTPSink_Write_NonOKStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	s, err := NewHTTPSink(HTTPSinkConfig{Name: "test", Endpoint: ts.URL})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	if err := s.Write(context.Background(), "line"); err == nil {
		t.Fatal("expected error for non-2xx status, got nil")
	}
}

func TestHTTPSink_Write_CustomHeaders(t *testing.T) {
	var gotHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Api-Key")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	s, err := NewHTTPSink(HTTPSinkConfig{
		Name:     "test",
		Endpoint: ts.URL,
		Headers:  map[string]string{"X-Api-Key": "secret"},
		Timeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	if err := s.Write(context.Background(), "data"); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if gotHeader != "secret" {
		t.Errorf("expected header value %q, got %q", "secret", gotHeader)
	}
}
