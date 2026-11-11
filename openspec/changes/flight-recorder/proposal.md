# Proposal: a flight recorder, so logs can be read after the fact

Status: proposed, not built. The design is in [design.md](design.md).

## Why

Bad behaviour is usually noticed after it has happened. A frozen pointer, a host that lost its place after sleeping: by the time the user thinks of attaching, the records that would explain it are gone. Writing log files would keep them, but files grow, need rotating and cleaning up, and leave what a program did lying on disk. This proposal keeps recent records in memory instead, in a ring buffer, and lets an attacher ask for them.

## What changes

- **A ring buffer in memory.** Recent records are kept, bounded by count and by bytes. The oldest are let go as new ones come. Nothing is written to disk, and everything goes when the process does.
- **Two places it can live, one way to ask.**
  - **In the watched program.** The library keeps a modest ring (by default INFO and above, 10 000 records or 8 MB), so the last minutes are there even if nothing was attached when it happened. This is the flight recorder proper.
  - **In a recorder.** `otel-attach record` runs as a long-lived attacher. It keeps a large ring (at DEBUG, say, a million records or 256 MB) and serves the same protocol on an endpoint of its own. For when the program's own ring is too small or too terse.
- **Asking for history.** An attacher can ask for what is kept, filtered by time, severity, scope and text, before (or instead of) following live records. The question is the same whether it goes to a program or a recorder. On the command line:

  ```
  otel-attach --dir ~/.unified-inputs --since 10m --level debug --target pointer_share
  ```
- **Warnings kept longer.** A second, small ring keeps only WARN and above, so a burst of debug records can't push out the warning that explains it.
- **Marks.** `otel-attach mark "pointer froze"` puts a record into the rings on the spot: a bookmark for the moment bad behaviour was spotted, to find later with `--around-mark`.

## Impact

- **The protocol:** new messages `query`, `history`, `history_end` and `mark`, and a `follow` field on `attach`. Version 1 stays: they are additions, and an older program answers `query` with nothing, which the attacher reports. See [specs/history/spec.md](specs/history/spec.md).
- **The watched library:** a ring, enabled and sized by `Config`. The filter then also enables the ring's level, so those records are built even with nothing attached. That is the cost of having them, and it's why the default is INFO.
- **The attacher:**
  - `--since`, `--until`, `--contains`, `--limit` and `--follow`;
  - `--around-mark`;
  - a `record` subcommand;
  - a `mark` subcommand.
- **No files.** Neither the program nor the recorder writes records to disk. A user who wants a copy redirects stdout.
