# netmon

A concurrent network health monitor and alerting CLI, written in Go.

`netmon` watches a list of targets — HTTP(S) endpoints or raw TCP ports — checks them all **concurrently** on a fixed interval, tracks their up/down state over time, and prints an alert only when a target's status actually **changes**. Think of it as a tiny, personal version of tools like Nagios or Prometheus's blackbox exporter.

> This project was built during my internship at **Softorage**, as a hands-on way to learn Go — specifically its concurrency model, including goroutines, channels, and mutex locks.

## Features

- ✅ Concurrent checks — all targets are checked at the same time using goroutines, not one by one
- ✅ HTTP(S) and raw TCP support — monitor websites as well as things like database or SSH ports
- ✅ Per-check timeouts — a single hung target can't block or stall the rest
- ✅ Repeating checks on a configurable interval
- ✅ State tracking — remembers whether each target was last up or down
- ✅ Change-based alerting — only prints output when a target's status flips (no spam on every tick)
- ✅ Graceful shutdown on `Ctrl+C`
- ✅ Simple JSON config — no recompiling needed to add or remove targets

## How it works

Every interval (default: 5 seconds), `netmon`:

1. Reads the target list from `targets.json`
2. Spins up a goroutine per target to check it concurrently
3. Collects results safely through a channel, using a `sync.WaitGroup` to know when all checks are done
4. Compares each result against the last known state (guarded by a `sync.Mutex` for safe concurrent access)
5. Prints an alert **only if** a target's status changed since the last check

## Requirements

- [Go](https://go.dev/dl/) 1.21 or later

## Getting started

Clone the repo and move into the project folder:

```bash
git clone https://github.com/<your-username>/netmon.git
cd netmon
```

Run it directly:

```bash
go run main.go
```

Or build a binary:

```bash
go build -o netmon
./netmon
```

## Configuration

Targets are defined in `targets.json`, in the same folder as the binary. Each entry has a friendly name, a `type` (`http` or `tcp`), and a `target` (a URL or a `host:port` pair).

```json
{
  "google": { "type": "http", "target": "https://google.com" },
  "github": { "type": "http", "target": "https://github.com" },
  "local_ssh": { "type": "tcp", "target": "127.0.0.1:22" },
  "my_db": { "type": "tcp", "target": "10.0.0.5:5432" }
}
```

- `type: "http"` — performs an HTTP GET request; considered up if the request completes (regardless of status code), down on connection/DNS/timeout errors.
- `type: "tcp"` — attempts a raw TCP connection to `host:port`; considered up if the connection opens successfully, down otherwise.

## Example output

```
netmon started. Checking every 5 seconds. Press Ctrl+C to stop.
[ALERT] google (https://google.com) is now UP (took 346.29ms)
[ALERT] github (https://github.com) is now UP (took 144.97ms)
[ALERT] local_ssh (127.0.0.1:22) is now DOWN: dial tcp 127.0.0.1:22: connectex: No connection could be made because the target machine actively refused it.

shutting down netmon...
```

After the first check, `netmon` stays quiet unless a target's status actually changes — no repeated output for targets that remain healthy (or remain down).

## Project status / roadmap

This project is being built incrementally as a learning exercise in Go concurrency and networking:

- [x] Stage 1 — Single target HTTP checker (status code + latency)
- [x] Stage 2 — Multiple targets checked concurrently via goroutines, channels, and `sync.WaitGroup`; targets loaded from a JSON config file
- [x] Stage 3 — Repeating checks via `time.Ticker`, per-check timeouts via `context.WithTimeout`, graceful shutdown via `os/signal`
- [x] Stage 4 — Per-target state tracking with `sync.Mutex`, alerts only on status change
- [x] Stage 5 — Raw TCP port checks alongside HTTP checks
- [ ] Stage 6 (stretch) — Persist history to SQLite; expose a `/metrics` endpoint (Prometheus format) or a simple HTML dashboard

## Tech / concepts used

- Goroutines & channels (concurrent checks)
- `sync.WaitGroup` (coordinating goroutine completion)
- `sync.Mutex` (safe shared state across goroutines)
- `context.WithTimeout` (per-request cancellation/timeouts)
- `time.Ticker` (repeating checks on an interval)
- `os/signal` (graceful shutdown on `Ctrl+C`)
- `encoding/json` (config file parsing)
- `net/http` and `net` (HTTP and raw TCP checks)

## License

MIT — feel free to use, modify, and learn from this.
