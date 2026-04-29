package pipeline

import (
	"strings"
	"time"
)

// TransformFunc is a function that transforms a log line.
// It returns the transformed line and a bool indicating whether to keep it.
type TransformFunc func(line string) (string, bool)

// TransformConfig holds configuration for the line transformer.
type TransformConfig struct {
	// Prefix prepends a static string to every line.
	Prefix string
	// StripANSI removes ANSI escape codes from lines.
	StripANSI bool
	// AddTimestamp prepends an RFC3339 timestamp to each line.
	AddTimestamp bool
}

// Transformer applies a chain of TransformFuncs to log lines.
type Transformer struct {
	funcs []TransformFunc
}

// NewTransformer builds a Transformer from a TransformConfig.
func NewTransformer(cfg TransformConfig) *Transformer {
	var fns []TransformFunc

	if cfg.StripANSI {
		fns = append(fns, stripANSIFunc)
	}
	if cfg.AddTimestamp {
		fns = append(fns, timestampFunc)
	}
	if cfg.Prefix != "" {
		prefix := cfg.Prefix
		fns = append(fns, func(line string) (string, bool) {
			return prefix + line, true
		})
	}

	return &Transformer{funcs: fns}
}

// Apply runs all transform functions on the given line.
// Returns the transformed line and false if the line should be dropped.
func (t *Transformer) Apply(line string) (string, bool) {
	for _, fn := range t.funcs {
		var keep bool
		line, keep = fn(line)
		if !keep {
			return "", false
		}
	}
	return line, true
}

// stripANSIFunc removes ANSI escape sequences from a line.
func stripANSIFunc(line string) (string, bool) {
	var b strings.Builder
	inEscape := false
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if ch == '\x1b' {
			inEscape = true
			continue
		}
		if inEscape {
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
				inEscape = false
			}
			continue
		}
		b.WriteByte(ch)
	}
	return b.String(), true
}

// timestampFunc prepends the current UTC time in RFC3339 format.
func timestampFunc(line string) (string, bool) {
	return time.Now().UTC().Format(time.RFC3339) + " " + line, true
}
