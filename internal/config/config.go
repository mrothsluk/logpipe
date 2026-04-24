package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the top-level logpipe configuration.
type Config struct {
	Inputs  []InputConfig  `yaml:"inputs"`
	Sinks   []SinkConfig   `yaml:"sinks"`
	Buffer  BufferConfig   `yaml:"buffer"`
}

// InputConfig describes a file to tail.
type InputConfig struct {
	Path  string            `yaml:"path"`
	Tags  map[string]string `yaml:"tags,omitempty"`
}

// SinkConfig describes an output destination.
type SinkConfig struct {
	Name    string            `yaml:"name"`
	Type    string            `yaml:"type"`
	Options map[string]string `yaml:"options,omitempty"`
}

// BufferConfig controls backpressure behaviour.
type BufferConfig struct {
	Capacity int `yaml:"capacity"`
	Workers  int `yaml:"workers"`
}

// Load reads and validates a YAML config file from the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Inputs) == 0 {
		return fmt.Errorf("config: at least one input is required")
	}
	for i, inp := range c.Inputs {
		if inp.Path == "" {
			return fmt.Errorf("config: input[%d]: path is required", i)
		}
	}
	if len(c.Sinks) == 0 {
		return fmt.Errorf("config: at least one sink is required")
	}
	for i, s := range c.Sinks {
		if s.Type == "" {
			return fmt.Errorf("config: sink[%d]: type is required", i)
		}
	}
	if c.Buffer.Capacity <= 0 {
		c.Buffer.Capacity = 4096
	}
	if c.Buffer.Workers <= 0 {
		c.Buffer.Workers = 2
	}
	return nil
}
