package sink_test

import (
	"context"
	"fmt"
	"time"

	"github.com/yourorg/logpipe/internal/pipeline"
	"github.com/yourorg/logpipe/internal/sink"
)

func ExampleNewRetrySinkWithStdout() {
	stdout, err := sink.NewStdoutSink("", false)
	if err != nil {
		panic(err)
	}
	cfg := pipeline.RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 50 * time.Millisecond,
		MaxDelay:     500 * time.Millisecond,
		Multiplier:   2.0,
	}
	rs := pipeline.NewRetrySink(stdout, cfg)
	defer rs.Close()

	if err := rs.Write(context.Background(), "hello from retry sink"); err != nil {
		fmt.Printf("write error: %v\n", err)
	}
	// Output:
	// hello from retry sink
}
