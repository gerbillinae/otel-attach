# Watched Specification

The library a program links to be watched. Code: `crates/watched` (`Watch`, its `tracing` layer, the log processor, the attach thread in `server.rs`).

## Requirements

### Requirement: One call to be watched

`Watch::start(Config { dir, name, version, instance })` SHALL bind the socket, write `attach.json` to `dir`, and return. Adding `watch.layer()` to the program's `tracing` subscriber is all that remains. If `instance` is not given, it SHALL be random. Dropping the `Watch` SHALL say `bye` to every attacher, stop the attach thread, and remove `attach.json`.

### Requirement: Events become OpenTelemetry records

The layer SHALL be OpenTelemetry's `tracing` bridge in front of an `SdkLoggerProvider` carrying the resource. Each event SHALL become a log record with:
- its severity (from its level);
- its target, as the record's scope;
- its message, as the body;
- its fields, as attributes;
- the time it happened.

### Requirement: Nothing nobody wants is built

The layer's filter SHALL enable only events at or above the most verbose severity any attacher wants, and none while none is attached. `tracing`'s cached interest SHALL be rebuilt whenever that changes, so an event disabled before anyone attached is enabled once someone wants it. The log processor SHALL also ask every attacher's filter (severity and targets) before a record is built.

#### Scenario: Nobody attached

- **WHEN** the program logs at any level with nothing attached
- **THEN** no record is built or queued

### Requirement: The program never waits

The log processor SHALL do no more than put the record on a bounded queue (8192), dropping and counting it when full. The attach thread SHALL drain it, and convert each record to OTLP once for all the attachers that want it.

### Requirement: Status pairs

`set_status(key, value)` and `clear_status(key)` SHALL change the pairs sent in `about`, and SHALL send it again when they actually change. They SHALL be callable from any thread.
