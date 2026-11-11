# Design: rings, and asking them

## The ring

A ring holds encoded records: each entry is a record's OTLP protobuf `LogRecord` bytes, with its scope, severity and time kept beside them for filtering without decoding. Storing records encoded keeps them compact (a typical record is 100 to 300 bytes) and means a query sends them as they are.

```rust
struct Entry {
    time: u64,           // unix nanoseconds
    severity: u8,
    scope: Arc<str>,     // interned: a few hundred scopes at most
    record: Bytes,       // OTLP LogRecord, protobuf
    sequence: u64,       // counts up from the program's start: gaps show what was let go
}

struct Ring {
    entries: VecDeque<Entry>,
    bytes: usize,
    most_entries: usize,
    most_bytes: usize,
    let_go: u64,         // entries let go since start
}
```

- **Bounds:** appending lets the oldest entries go until both bounds hold.
- **Locking:** the ring sits behind a mutex, held only to push or to copy out a query's entries.
- **Time:** time only ever increases within a ring. Each entry is stamped when the attach thread takes it off the queue, so a query by time is a binary search.
- **Two rings:**
  - the main ring, holding everything at its level;
  - the warnings ring, holding WARN and above at a tenth of the main ring's size (by default).

  A query reads both and merges by `sequence`, without repeats.

### In the watched program

```rust
Config {
    ...,
    ring: Some(RingConfig { min_severity: INFO, entries: 10_000, bytes: 8 << 20 }),
}
```

- **Feeding it:** the attach thread appends every record at or above the ring's level, whatever is attached.
- **The filter:** it enables the more verbose of the ring's level and the attachers'. With the ring at INFO, INFO events cost a record each, even unwatched. DEBUG and TRACE still cost nothing until asked for.
- **A burst:** if a burst fills the queue faster than the attach thread drains it, the ring misses those records too. The ring then holds a `dropped` entry for each gap, so a query shows where records are missing.

### In a recorder

`otel-attach record --dir ~/.unified-inputs --level debug --entries 1000000 --bytes 256M`:
1. It attaches to the program like any attacher, at its own level.
2. It appends what it receives to its rings.
3. It serves the protocol itself, on its own ROUTER socket and endpoint, written as `attach.recorder.json` next to the program's `attach.json`.

So an attacher talks to the recorder exactly as to the program:
- `about` is the program's, with `recorder.*` pairs added to the status (entries kept, oldest kept);
- `logs` relays what the program sends;
- `query` answers from the recorder's rings.

`otel-attach --dir ~/.unified-inputs --recorder` reads `attach.recorder.json` instead of `attach.json`.

The recorder survives the program being replaced. It reattaches to the new copy as any attacher does, and keeps the old copy's records. Each entry's resource (its `service.instance.id`) says which copy it came from. A recorder can follow only one program; one program can have any number of recorders.

## Asking

### Messages

| Type | From | Body | Meaning |
| --- | --- | --- | --- |
| `attach` | attacher | as now, plus `follow` (default `true`) | `follow: false`: attach for queries only; no live records. |
| `query` | attacher | `{id, since, until, min_severity, targets, contains, limit, around_mark}` | "Send me what you keep that matches." `since` and `until` are unix nanoseconds (the attacher turns `10m` into one). `contains` matches the body text, case-insensitively. `limit` keeps the newest that many (default 10 000). `around_mark`: the entries within a minute either side of the last mark whose text contains this. |
| `history` | watched / recorder | OTLP `ExportLogsServiceRequest` | Matching records, oldest first, in batches of up to 256. A resource per program copy the records came from. |
| `history_end` | watched / recorder | `{id, count, oldest, let_go, truncated}` | The answer is complete. `count` is the records sent. `oldest` is the time of the oldest entry kept, whatever matched. `let_go` counts the entries let go before `oldest`. `truncated` says `limit` cut the answer. |
| `mark` | attacher | `{text}` | Put a mark in the rings now: a record at INFO, scope `otel_attach::mark`, body `text`. It is also sent live to followers. |

### Order

An `attach` with `follow: true` and a `query` together means history, then live, with nothing lost between them:
1. The server answers the query from the ring up to the newest `sequence` it holds.
2. It then sends live records from the next `sequence` on.

The two never overlap and leave no gap. The attach thread is one task, and it doesn't take new records off the queue while it answers a query. The queue holds them meanwhile, and a query of 10 000 records takes milliseconds to send.

### The attacher

```
otel-attach --dir D [--recorder] [--since 10m] [--until 2m] [--contains term] [--limit N]
            [--around-mark "pointer froze"] [--follow] [--level ...] [--target ...] [--json]
otel-attach record --dir D [--level debug] [--entries N] [--bytes SIZE]
otel-attach mark --dir D "text"
```

- **Query or follow:** with any of `--since`, `--until`, `--contains`, `--around-mark`, it queries. It prints the history, then a line `# history: 412 records from 15:02:03 (1 200 let go before)`, then exits, or follows with `--follow`. Without them, it follows as today.
- **`mark`:** attaches with `follow: false`, sends the mark, detaches. It goes to the recorder too, if one is running, so the moment is marked wherever the records are kept.

## Why not files

- **Disk:** a file needs rotating, cleaning up and a place to live, and it leaves what the program did on disk. A ring needs none of that.
- **Its limit:** a ring is gone when the process is. With the recorder that only matters if the recorder itself goes. A user who wants a copy has `otel-attach --since 1h > copy.log`: the choice to write a file is theirs, made at the moment it's wanted.
