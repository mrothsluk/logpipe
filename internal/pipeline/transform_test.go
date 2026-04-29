package pipeline

import (
	"strings"
	"testing"
)

func TestTransformer_NoOps(t *testing.T) {
	tr := NewTransformer(TransformConfig{})
	out, keep := tr.Apply("hello world")
	if !keep {
		t.Fatal("expected line to be kept")
	}
	if out != "hello world" {
		t.Fatalf("expected unchanged line, got %q", out)
	}
}

func TestTransformer_Prefix(t *testing.T) {
	tr := NewTransformer(TransformConfig{Prefix: "[app] "})
	out, keep := tr.Apply("started")
	if !keep {
		t.Fatal("expected line to be kept")
	}
	if out != "[app] started" {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestTransformer_StripANSI(t *testing.T) {
	tr := NewTransformer(TransformConfig{StripANSI: true})
	// bold red "ERROR" followed by reset
	input := "\x1b[1;31mERROR\x1b[0m: something failed"
	out, keep := tr.Apply(input)
	if !keep {
		t.Fatal("expected line to be kept")
	}
	if strings.Contains(out, "\x1b") {
		t.Fatalf("ANSI codes not stripped, got: %q", out)
	}
	if !strings.Contains(out, "ERROR") {
		t.Fatalf("text content lost, got: %q", out)
	}
}

func TestTransformer_AddTimestamp(t *testing.T) {
	tr := NewTransformer(TransformConfig{AddTimestamp: true})
	out, keep := tr.Apply("ping")
	if !keep {
		t.Fatal("expected line to be kept")
	}
	// RFC3339 timestamps start with a 4-digit year
	if len(out) < 20 || out[4] != '-' {
		t.Fatalf("expected RFC3339 prefix, got: %q", out)
	}
	if !strings.HasSuffix(out, " ping") {
		t.Fatalf("original line not preserved, got: %q", out)
	}
}

func TestTransformer_ChainsInOrder(t *testing.T) {
	// StripANSI runs before Prefix is applied
	tr := NewTransformer(TransformConfig{
		StripANSI: true,
		Prefix:    ">> ",
	})
	input := "\x1b[32mOK\x1b[0m"
	out, keep := tr.Apply(input)
	if !keep {
		t.Fatal("expected line to be kept")
	}
	if out != ">> OK" {
		t.Fatalf("unexpected chained output: %q", out)
	}
}

func TestStripANSIFunc_PlainText(t *testing.T) {
	out, keep := stripANSIFunc("plain text")
	if !keep || out != "plain text" {
		t.Fatalf("plain text should pass through unchanged, got %q", out)
	}
}
