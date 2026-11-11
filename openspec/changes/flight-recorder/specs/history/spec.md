# History Specification (delta: added)

## ADDED Requirements

### Requirement: Recent records are kept in memory, never on disk

A watched program configured with a ring, and a recorder, SHALL keep the records at or above the ring's level in memory, bounded by count and bytes, letting the oldest go first. A second ring SHALL keep WARN and above longer. Neither SHALL write records to disk.

#### Scenario: A burst of debug records

- **WHEN** a burst of debug records fills the main ring
- **THEN** the warnings from before the burst are still kept, in the warnings ring

### Requirement: An attacher can ask for what is kept

A `query` SHALL be answered with the kept records matching its time range, severity, targets and text, oldest first, at most `limit` (the newest kept), as `history` batches, then `history_end`. `history_end` says how many were sent, the time of the oldest kept, how many were let go before it, and whether the limit cut the answer.

#### Scenario: Looking back after the pointer froze

- **WHEN** the user runs `otel-attach --dir ~/.unified-inputs --since 5m --level info --target pointer_share`
- **THEN** pointer sharing's records from the last five minutes are printed, then how far back the ring goes

### Requirement: History, then live, with nothing between

An `attach` with a `query` and `follow: true` SHALL be sent the history up to the newest record kept, then live records from the next one, none twice and none missed.

### Requirement: A recorder speaks the same protocol

`otel-attach record` SHALL attach to a program, keep what it receives in its rings, and serve the protocol on an endpoint of its own, written to `attach.recorder.json` beside the program's `attach.json`. It SHALL relay live records and the program's `about`, with its own `recorder.*` status pairs added. It SHALL keep records across the program being replaced.

### Requirement: Marks

A `mark {text}` SHALL put a record in the rings at once (INFO, scope `otel_attach::mark`) and send it live to followers. `query` SHALL find the entries within a minute either side of the last mark containing given text.
