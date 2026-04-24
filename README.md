# logpipe

Lightweight log aggregation daemon that tails files and ships to multiple sinks with backpressure support.

---

## Installation

```bash
go install github.com/yourorg/logpipe@latest
```

Or build from source:

```bash
git clone https://github.com/yourorg/logpipe.git && cd logpipe && go build -o logpipe .
```

---

## Usage

Create a config file (`logpipe.yaml`) and start the daemon:

```yaml
sources:
  - path: /var/log/app/*.log
    format: json

sinks:
  - type: elasticsearch
    url: http://localhost:9200
    index: app-logs
  - type: stdout
```

```bash
logpipe --config logpipe.yaml
```

Logpipe will tail all matching files, parse entries, and fan-out to each configured sink. If a sink falls behind, backpressure is applied to prevent memory exhaustion — no log lines are silently dropped.

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `logpipe.yaml` | Path to config file |
| `--log-level` | `info` | Log verbosity (`debug`, `info`, `warn`, `error`) |
| `--dry-run` | `false` | Parse and validate config without starting |

---

## Contributing

Pull requests are welcome. Please open an issue first to discuss significant changes.

---

## License

[MIT](LICENSE)