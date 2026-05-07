package sink_test

import (
	"context"
	"fmt"
	"os"

	"github.com/yourorg/logpipe/internal/pipeline"
	"github.com/yourorg/logpipe/internal/sink"
)

// ExampleNewHeaderSinkWithStdout demonstrates wrapping a StdoutSink with a
// HeaderSink so that a banner line is emitted before the first log entry.
func ExampleNewHeaderSinkWithStdout() {
	stdout, err := sink.NewStdoutSink(sink.StdoutConfig{Writer: os.Stdout})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer stdout.Close()

	cfg := pipeline.HeaderConfig{
		Header:      "=== logpipe session start ===",
		RepeatEvery: 0,
	}
	h, err := pipeline.NewHeaderSink(stdout, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer h.Close()

	ctx := context.Background()
	_ = h.Write(ctx, "server started")
	_ = h.Write(ctx, "listening on :8080")

	// Output:
	// === logpipe session start ===
	// server started
	// listening on :8080
}
