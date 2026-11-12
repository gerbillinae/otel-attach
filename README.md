# otel-attach

Watch a running program's logs, without log files: attach to it and print them, or have a service keep them in memory for an agent to read back.

The program links `otel-attach-watched` and adds its `tracing` layer. It then listens on the loopback address, and writes where in `attach.json` in a directory of its own. Records are OpenTelemetry log records, sent as OTLP protobuf. Nothing is recorded while nothing is attached.

```rust
use tracing_subscriber::prelude::*;

let watch = otel_attach_watched::Watch::start(otel_attach_watched::Config {
    dir: config_dir,
    name: "my-program".into(),
    version: env!("CARGO_PKG_VERSION").into(),
    instance: None,
})?;
tracing_subscriber::registry().with(watch.layer()).init();
watch.set_status("role", "owner");
```

## Watching by hand

```
$ otel-attach watch --dir ~/.my-program --level debug --target my_program::net
# attached to tcp://127.0.0.1:51234
# my-program 1.0.0 on studio (pid 4242, instance 9e1f…)
# status: role=owner
2026-11-11T15:02:03.123Z DEBUG my_program::net  connected  peer=3fa2-91c0-7d
```

`--json` writes one OTLP JSON object per line instead.

## The service, for agents

`otel-attach serve` runs until stopped. It serves MCP at `http://127.0.0.1:47400/mcp`, behind a token kept in `~/.otel-attach/token`. To add it to Claude Code:

```
claude mcp add --transport http otel-attach http://127.0.0.1:47400/mcp \
  --header "Authorization: Bearer $(cat ~/.otel-attach/token)"
```

The tools:
- `list_processes`: what is watched, with each program's name and version as it reports them, and how much of its log is kept.
- `add_process`: watch a program, by its directory. A program already watched is refused.
- `remove_process`: stop watching a program.
- `query_logs`: read the log back, by time (`since`/`until`: RFC 3339 or `10m`) or by line (`from_line`/`to_line`), narrowed by level, scope and text. Line numbers are never reused.
- `set_log_level`: change what a program sends, from now on.
- `set_buffer_size`: change how many lines are kept, from 100 to 1 000 000.

What is watched is saved, and watched again when the service restarts. The logs themselves stay in memory only.

The protocol, the library, the attacher and the service are specified in [openspec/](openspec/project.md).

## Layout

- `crates/protocol`: the messages, filters, severities and the discovery file. Pure.
- `crates/watched`: the library a program links.
- `crates/client`: attaching: finding the program, following new copies, reporting events.
- `crates/ring`: the ring buffer, numbered by line, queried by line or time. Pure.
- `apps/otel-attach`: `watch` and `serve`.
