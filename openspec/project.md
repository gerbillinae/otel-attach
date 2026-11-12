# otel-attach: project context

otel-attach lets a separate program attach to a running one and watch what it does. The attacher connects over ZeroMQ, says which log level it wants, and receives the program's `tracing` events as OpenTelemetry log records, along with who the program is and how it stands. `otel-attach watch` writes them to stdout. `otel-attach serve` is a long-running service: it keeps each watched program's recent records in a ring buffer, and an agent manages and queries it through MCP tools. No log files are involved. It is a Rust workspace, pure Rust throughout, so it builds for macOS, Linux and Windows from any of them.

## How this folder is organised

- `specs/<capability>/spec.md`: what is built, as requirements (SHALL) with scenarios (WHEN / THEN).
- `changes/<change-id>/`: proposals not built yet (`proposal.md`, `design.md`, `tasks.md`, and the spec deltas they would make). None are open.

## Capabilities

| Spec | Covers | Code |
| --- | --- | --- |
| `protocol` | The messages, the discovery file, severities, liveness, versioning | `crates/protocol` |
| `watched` | The library a program links to be watched: the `tracing` layer, cost, queueing, status | `crates/watched` |
| `attacher` | `otel-attach watch`: finding, attaching, reattaching, output | `apps/otel-attach/src/watch.rs`, `crates/client` |
| `service` | `otel-attach serve`: the MCP tools, ring buffers, line numbers, limits, the token | `apps/otel-attach/src/{mcp,monitors}.rs`, `crates/ring` |

## Conventions

- **The watched program never waits on an attacher.** Queues are bounded, and sends time out; what doesn't fit is dropped, counted and reported.
- **Nothing costs anything until someone asks.** Levels nobody wants stay disabled at the `tracing` call site.
- **Local and private.** Loopback TCP only, and a token in a file only the user can read.
- **Records are OpenTelemetry's.** OTLP protobuf on the wire, so they can go on to an OpenTelemetry collector unchanged.

## Commands

- `cargo test`: unit tests, a watched program and an attacher over real sockets, and an MCP client driving the service over real HTTP.
- `cargo clippy --all-targets -- -D warnings`, and again with `--target x86_64-pc-windows-msvc`.
- `cargo run --example demo -p otel-attach-watched -- /tmp/demo`, then `cargo run -p otel-attach -- watch --dir /tmp/demo --level debug`: try it by hand. `cargo run -p otel-attach -- serve` runs the service.
