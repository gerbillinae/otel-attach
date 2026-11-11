# Protocol Specification

How a watched program and an attacher talk. Code: `crates/protocol` (messages, filters, severities, the discovery file), with the sockets in `crates/watched` and `apps/otel-attach`.

## Requirements

### Requirement: ZeroMQ ROUTER and DEALER over loopback TCP

The watched program SHALL bind a ZeroMQ ROUTER socket on `tcp://127.0.0.1`, on a port the OS chooses. An attacher SHALL connect a DEALER. ROUTER sees each attacher's identity, so each has its own filter and is answered directly. Loopback TCP is used rather than IPC, which zmq.rs offers only as Unix sockets.

### Requirement: The discovery file says where to attach

The watched program SHALL write `attach.json` to a directory of its own, holding:
- `protocol`;
- `endpoint`;
- `token`: 32 random bytes in hex, new at every start;
- `pid`.

It SHALL write the file whole (a temporary file renamed over it), readable by the user alone where the system has modes, and remove it on exit if it still names its own `pid`. An attacher SHALL present the token. Another user can reach the port but cannot read the token.

#### Scenario: Another user attaches

- **WHEN** a process without the token sends `attach`
- **THEN** it gets `refused`, saying the token is wrong, and nothing else

### Requirement: Messages are a type frame and a body frame

Every message SHALL be a ZeroMQ multipart message: an ASCII type, then a body (on the ROUTER side, the attacher's identity frame comes first). Bodies are JSON, except `logs`, which is an OTLP protobuf `ExportLogsServiceRequest`.

Attacher to watched:
- `attach {protocol, token, min_severity, targets, name}`;
- `level {min_severity, targets}`;
- `ping {}`;
- `detach {}`.

Watched to attacher:
- `welcome {protocol}`;
- `refused {reason, protocol}`;
- `about {resource, status}`;
- `logs`;
- `dropped {count}`;
- `pong {}`;
- `bye {reason}`.

A message type or field a side doesn't know SHALL be ignored.

### Requirement: Attaching

The watched program SHALL answer an `attach` with the right token and its protocol version with `welcome`, then `about`. Any other `attach` it SHALL answer with `refused`, saying why and which version it speaks. `level` changes an attached filter from then on. `detach` ends the attachment.

### Requirement: What an attacher gets

An attacher SHALL be sent only the records at or above its `min_severity`, and, if it gave `targets`, only those whose scope (the `tracing` target, a Rust module path) starts with one of them. Records go in batches at most 100 ms apart, or of 256 records, whichever comes first. Within a batch, records are grouped by scope under one resource.

### Requirement: Severities are OpenTelemetry's

Severities SHALL be OpenTelemetry severity numbers:

| Name | Numbers | `tracing` level |
| --- | --- | --- |
| TRACE | 1 to 4 | `trace` |
| DEBUG | 5 to 8 | `debug` |
| INFO | 9 to 12 | `info` |
| WARN | 13 to 16 | `warn` |
| ERROR | 17 to 20 | `error` |
| FATAL | 21 to 24 | (none) |

### Requirement: Who the program is, and how it stands

`about` SHALL carry:
- `resource`, OpenTelemetry resource attributes: `service.name`, `service.version`, `service.instance.id`, `process.pid`, `host.name`, `os.type`, `process.start_time` (RFC 3339);
- `status`, the program's own key/value pairs.

It SHALL be sent after `welcome`, and again, at most every 250 ms, whenever the status changes. Every `logs` batch carries the same resource.

### Requirement: Liveness

An attacher SHALL send `ping` every 2 s, and the watched program SHALL answer each with `pong`. The watched program SHALL forget an attacher not heard from for 6 s.

The watched program SHALL send `bye` to every attacher before it exits, and give it 200 ms to leave. An attacher that gets `bye`, hears nothing for 6 s, or finds that `attach.json` names another copy, SHALL attach again, to the new copy if there is one.

### Requirement: Dropped records are reported

A record that does not fit the watched program's queue, or whose batch an attacher was not reading fast enough to take (a send waiting more than 100 ms), SHALL be dropped and counted. The count SHALL be sent to the attacher as `dropped` within 100 ms.
