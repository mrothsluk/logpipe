// Package main is the entry point for the logpipe daemon.
// It wires together configuration, tail managers, pipeline stages,
// and sinks into a running log aggregation process.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourorg/logpipe/internal/config"
	"github.com/yourorg/logpipe/internal/pipeline"
	"github.com/yourorg/logpipe/internal/sink"
	"github.com/yourorg/logpipe/internal/tail"
)

var (
	cfgPath = flag.String("config", "config.yaml", "path to configuration file")
	version = "dev" // injected at build time via -ldflags
)

func main() {
	flag.Parse()

	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Printf("logpipe %s\n", version)
		os.Exit(0)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	sinks, err := buildSinks(cfg)
	if err != nil {
		log.Fatalf("failed to build sinks: %v", err)
	}
	defer closeSinks(sinks)

	router := pipeline.NewRouter(sinks)

	queue := pipeline.NewBoundedQueue(pipeline.DefaultBackpressureConfig(), router)
	defer queue.Close()

	mgr := tail.NewManager(cfg, queue)

	log.Printf("logpipe %s starting — tailing %d input(s), routing to %d sink(s)",
		version, len(cfg.Inputs), len(sinks))

	if err := mgr.Start(ctx); err != nil {
		log.Fatalf("tail manager exited with error: %v", err)
	}

	log.Println("logpipe shut down cleanly")
}

// buildSinks constructs a Sink for each entry in the configuration.
func buildSinks(cfg *config.Config) ([]sink.Sink, error) {
	var sinks []sink.Sink

	for _, sc := range cfg.Sinks {
		var s sink.Sink
		var err error

		switch sc.Type {
		case "stdout":
			s, err = sink.NewStdoutSink(sc)
		case "http":
			s, err = sink.NewHTTPSink(sc)
		case "file":
			s, err = sink.NewFileSink(sc)
		default:
			return nil, fmt.Errorf("unknown sink type %q", sc.Type)
		}

		if err != nil {
			return nil, fmt.Errorf("sink %q: %w", sc.Type, err)
		}

		// Wrap every sink with retry behaviour so transient errors are handled
		// automatically without special-casing in the pipeline.
		s = pipeline.NewRetrySink(s, pipeline.DefaultRetryConfig())

		log.Printf("registered sink: %s", s.Name())
		sinks = append(sinks, s)
	}

	if len(sinks) == 0 {
		return nil, fmt.Errorf("no sinks configured")
	}

	return sinks, nil
}

// closeSinks calls Close on every sink, logging but not halting on errors.
func closeSinks(sinks []sink.Sink) {
	for _, s := range sinks {
		if err := s.Close(); err != nil {
			log.Printf("error closing sink %s: %v", s.Name(), err)
		}
	}
}
