package sink_test

import (
	"context"
	"fmt"
	"time"

	"logpipe/internal/pipeline"
	"logpipe/internal/sink"
)

func ExampleNewCircuitBreakerSinkWithStdout() {
	stdout, err := sink.NewStdoutSink("", false)
	if err != nil {
		panic(err)
	}

	cfg := pipeline.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenDuration:     5 * time.Second,
	}
	cb := pipeline.NewCircuitBreakerSink(stdout, cfg)

	ctx := context.Background()
	if err := cb.Write(ctx, "hello from circuit breaker"); err != nil {
		fmt.Println("write error:", err)
		return
	}

	fmt.Println("state closed:", cb.State() == pipeline.StateClosed)
	// Output:
	// hello from circuit breaker
	// state closed: true
}
