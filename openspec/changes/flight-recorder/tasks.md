# Tasks: a flight recorder

## 1. The ring (`crates/protocol` or a new `crates/ring`, pure)

- [ ] `Ring`: encoded entries with time, severity, scope and sequence; bounds by count and bytes; queries by time (binary search), severity, targets and text; tests for bounds, order and gaps.
- [ ] The warnings ring, and merging the two by sequence.

## 2. The protocol

- [ ] `query`, `history`, `history_end`, `mark`, and `follow` on `attach`; round-trip tests.

## 3. In the watched library

- [ ] `Config::ring`; the attach thread appending to it; the filter enabling its level.
- [ ] Answering `query`, then going on live, with nothing lost between; marks.
- [ ] A test: records logged with nothing attached, then a query finds them.

## 4. The attacher

- [ ] `--since`, `--until`, `--contains`, `--limit`, `--around-mark`, `--follow`, `--recorder`.
- [ ] `otel-attach mark`.
- [ ] `otel-attach record`: attach, keep, serve on `attach.recorder.json`, reattach across replacements.

## 5. Docs

- [ ] Move the history spec into `specs/`; update `protocol`, `watched` and `attacher`.
