# otel-attach: project context

otel-attach lets a separate program attach to a running one and watch what it does. The attacher connects over ZeroMQ, says which log level it wants, and receives the program's `tracing` events as OpenTelemetry log records, along with who the program is and how it stands. It writes them to stdout. No log files are involved. It is a Rust workspace, pure Rust throughout, so it builds for macOS, Linux and Windows from any of them.

## How this folder is organised

- `specs/<capability>/spec.md`: what is built, as requirements (SHALL) with scenarios (WHEN / THEN).
- `changes/<change-id>/`: proposals not built yet (`proposal.md`, `design.md`, `tasks.md`, and the spec deltas they would make). Open: `flight-recorder`.

## Capabilities

| Spec | Covers | Code |
| --- | --- | --- |
| `protocol` | The messages, the discovery file, severities, liveness, versioning | `crates/protocol` |
| `watched` | The library a program links to be watched: the `tracing` layer, cost, queueing, status | `crates/watched` |
| `attacher` | The `otel-attach` program: finding, attaching, reattaching, output | `apps/otel-attach` |

## Conventions

- **The watched program never waits on an attacher.** Queues are bounded, and sends time out; what doesn't fit is dropped, counted and reported.
- **Nothing costs anything until someone asks.** Levels nobody wants stay disabled at the `tracing` call site.
- **Local and private.** Loopback TCP only, and a token in a file only the user can read.
- **Records are OpenTelemetry's.** OTLP protobuf on the wire, so they can go on to an OpenTelemetry collector unchanged.

## Commands

- `cargo test`: the protocol's unit tests, and a watched program and an attacher talking over real sockets.
- `cargo clippy --all-targets -- -D warnings`, and again with `--target x86_64-pc-windows-msvc`.
- `cargo run --example demo -p otel-attach-watched -- /tmp/demo`, then `cargo run -p otel-attach -- --dir /tmp/demo --level debug`: try it by hand.
