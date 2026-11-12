# Service Specification

`otel-attach serve`: a long-running service that watches programs for an agent. It attaches to the programs it's told to, keeps each one's recent records in a ring buffer in memory, and offers MCP tools to manage what is watched and read the logs back. No log files are written. Code: `apps/otel-attach/src/{main.rs, mcp.rs, monitors.rs}`, with `crates/ring` (the buffer) and `crates/client` (attaching).

## Requirements

### Requirement: An MCP server on the loopback address, behind a token

The service SHALL serve MCP over streamable HTTP at `http://127.0.0.1:<port>/mcp`. The port defaults to 47400, fixed so an agent's configuration keeps working. Every request SHALL carry `Authorization: Bearer <token>`; a request without it is answered 401. The token SHALL be made once and kept in `<state>/token` (default `~/.otel-attach/token`), readable by the user alone. Only loopback `Host` headers SHALL be accepted, against DNS rebinding. At start, the service SHALL print the URL and the command that adds it to Claude Code.

#### Scenario: Another program on the machine

- **WHEN** a request comes without the token
- **THEN** it is refused, and no tool runs

### Requirement: Listing what is watched

`list_processes` SHALL return every watched process, with:
- its id, which the other tools take as `process`;
- the name and version it reports (`service.name`, `service.version`), and its pid, host, instance and status pairs;
- how it is attached ("attached", "waiting for the program to start", "attaching again: …", "refused: …");
- the level and scopes asked of it;
- its buffer: capacity in lines, lines and bytes kept, first and last line numbers, oldest and newest times;
- how many records the process said it dropped.

#### Scenario: What an agent sees

- **WHEN** an agent lists the processes while unified-inputs is watched
- **THEN** it sees `unified-inputs` with its version, as the program reported them, and how much of its log is kept

### Requirement: Adding a process

`add_process` SHALL start watching a program given by its directory (holding `attach.json`) or by endpoint and token. It also takes a level (default info), scopes, and buffer size in lines (default 10 000). It SHALL refuse a program already watched: the same directory, however it was written (`~`, relative, a link), or the same endpoint. A program given by directory SHALL be followed across its restarts. A program not running yet SHALL be waited for.

#### Scenario: Added twice

- **WHEN** an agent adds `~/.unified-inputs` while it is already watched
- **THEN** the call is refused, saying it is already watched

### Requirement: Removing a process

`remove_process` SHALL stop watching a process, detach from it and let its buffer go.

### Requirement: Reading the log back by time or by line

`query_logs` SHALL return a process's kept records matching:
- a time range: `since` and `until`, each RFC 3339 or a duration ago such as `10m`;
- a line range: `from_line` and `to_line`, inclusive;
- or both;
- narrowed by `level`, scope prefixes (`targets`) and text (`contains`, any case, in the body or attributes).

Each record gets a line number when it is kept, counting up from 1 for as long as the process is watched, never reused, so a line range means the same records whenever it is asked for. At most `limit` lines SHALL be returned (default 200, at most 2000):
- reading forward from a line or a time, the first that match;
- otherwise, the newest.

The answer SHALL say which lines are kept, how many were let go, and, when cut by the limit, the `from_line` or `to_line` that continues it. Each record is one line: `#line time severity scope body key=value…`.

#### Scenario: Looking back after bad behaviour

- **WHEN** an agent asks for the last 10 minutes of `pointer_share` records at debug
- **THEN** it gets them oldest first with their line numbers, and can ask for the lines around any one of them

### Requirement: Changing the level

`set_log_level` SHALL change the level (and scopes) asked of a process, from then on, by telling the process (`level` in the attach protocol). Records already kept stay. The level is asked for again whenever the program is reattached.

### Requirement: Changing the buffer size

`set_buffer_size` SHALL set how many lines a process's buffer keeps. It SHALL refuse a size outside 100 to 1 000 000 lines. Shrinking lets the oldest lines go at once. Whatever the size in lines, a buffer SHALL NOT hold more than 256 MB of text, and a record's text is cut at 4 KB.

### Requirement: What is watched survives a restart of the service

The service SHALL save what it watches (target, level, scopes, buffer size) to `<state>/processes.json`, readable by the user alone, whenever that changes, and watch the same again when it starts. The buffers themselves are not saved.
