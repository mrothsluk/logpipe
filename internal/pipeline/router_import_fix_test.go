package pipeline

import "github.com/yourorg/logpipe/internal/sink"

// Ensure mockSink satisfies the sink.Sink interface at compile time.
var _ sink.Sink = (*mockSink)(nil)
