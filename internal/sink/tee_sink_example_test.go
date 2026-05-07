package sink_test

import (
	"context"
	"fmt"
	"os"

	"github.com/yourorg/logpipe/internal/pipeline"
	"github.com/yourorg/logpipe/internal/sink"
)

// ExampleNewTeeSinkWithStdout demonstrates tee-ing log lines to both stdout
// and a file sink simultaneously.
func ExampleNewTeeSinkWithStdout() {
	stdoutSink, err := sink.NewStdoutSink(sink.StdoutConfig{Writer: os.Stdout})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	tmpFile, _ := os.CreateTemp("", "logpipe-tee-*.log")
	defer os.Remove(tmpFile.Name())

	fileSink, err := sink.NewFileSink(sink.FileConfig{Path: tmpFile.Name()})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	tee, err := pipeline.NewTeeSink(stdoutSink, fileSink)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer tee.Close()

	_ = tee.Write(context.Background(), "hello from tee")
	// Output:
	// hello from tee
}
