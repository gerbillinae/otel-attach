# otel-attach

Attach to a running program and watch its logs, without log files.

The program links `otel-attach-watched` and adds its `tracing` layer. It then listens on the loopback address, and writes where in `attach.json` in a directory of its own. `otel-attach` connects over ZeroMQ, says which level it wants, and writes the program's records to stdout. The records are OpenTelemetry log records, sent as OTLP protobuf. Nothing is recorded while nothing is attached.

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

```
$ otel-attach --dir ~/.my-program --level debug --target my_program::net
# attached to tcp://127.0.0.1:51234
# my-program 1.0.0 on studio (pid 4242, instance 9e1f…)
# status: role=owner
2026-11-11T15:02:03.123Z DEBUG my_program::net  connected  peer=3fa2-91c0-7d
```

`--json` writes one OTLP JSON object per line instead. The protocol, the library and the program are specified in [openspec/](openspec/project.md).

## Layout

- `crates/protocol`: the messages, filters, severities and the discovery file. Pure.
- `crates/watched`: the library a program links: the `tracing` layer, the OpenTelemetry logger provider, and the ZeroMQ server on a thread of its own.
- `apps/otel-attach`: the attacher.
