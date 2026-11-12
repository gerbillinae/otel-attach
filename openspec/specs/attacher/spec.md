# Attacher Specification

`otel-attach watch`: attach to one program and print its records. Code: `apps/otel-attach/src/watch.rs`, with the attaching itself in `crates/client`, which the service shares.

## Requirements

### Requirement: Finding the program

`otel-attach watch --dir <dir>` (or `OTEL_ATTACH_DIR`) SHALL read `attach.json` from the program's directory. `--endpoint` and `--token` name one directly. While no program is found, it SHALL say so once and keep looking every 500 ms.

### Requirement: What it asks for

`--level` (default `info`) SHALL set the minimum severity, by name. Each `--target` adds a scope prefix.

### Requirement: Staying attached

It SHALL ping every 2 s, and attach again when:
- the program says `bye`;
- nothing has come for 6 s;
- `attach.json` names another copy.

It SHALL say which on a line of its own. It SHALL send `detach` when interrupted, and exit. A refusal ends it when given `--endpoint`. With `--dir`, it waits for a copy that will take it.

#### Scenario: The program is replaced

- **WHEN** a new copy of the watched program starts while attached to the old one
- **THEN** it says a new copy started, attaches to it, and goes on printing its records

### Requirement: Output

It SHALL write one line per record to stdout:

```
2026-11-11T15:02:03.123Z INFO  scope  body  key=value ...
```

That is the time, severity, scope, body, then the attributes. `about` SHALL print as `# <name> <version> on <host> (pid …, instance …)` the first time, and `# status: key=value …` whenever the status changes. Notices (attached, dropped, reattaching) SHALL print as `#` lines.

With `--json`, every one of these SHALL be a JSON object on its own line:
- a record as an OTLP JSON `LogRecord` with an added `scope`;
- `about` as `{"type": "about", "resource", "status"}`;
- a notice as `{"type": "note", "text"}`.
