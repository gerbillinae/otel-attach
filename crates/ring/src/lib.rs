//! A ring buffer of log records in memory (openspec `service`): the newest kept, the oldest let go
//! once it holds more lines or bytes than it may. Every record gets a line number, counting up
//! from 1 for as long as the ring lives and never reused, so a range of lines means the same
//! records whenever it is asked for. Pure: no I/O, no clock.

use std::collections::VecDeque;

/// The fewest and most lines a ring may hold, and the most bytes (the text of its records).
pub const FEWEST_LINES: usize = 100;
pub const MOST_LINES: usize = 1_000_000;
pub const MOST_BYTES: usize = 256 << 20;
/// A record's body and attributes past this are cut, so one record cannot take a ring's room.
pub const MOST_RECORD_BYTES: usize = 4096;

/// One record, as kept.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub line: u64,
    /// When it happened, unix nanoseconds.
    pub time: u64,
    /// OpenTelemetry severity number.
    pub severity: u8,
    /// Its scope: the module path that logged it.
    pub scope: String,
    pub body: String,
    pub attributes: Vec<(String, String)>,
}

impl Entry {
    fn bytes(&self) -> usize {
        self.scope.len()
            + self.body.len()
            + self
                .attributes
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
            + 48
    }
}

/// A record on its way in: the ring numbers it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Record {
    pub time: u64,
    pub severity: u8,
    pub scope: String,
    pub body: String,
    pub attributes: Vec<(String, String)>,
}

/// What to find. Line and time bounds are inclusive; any left out is unbounded.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Query {
    pub from_line: Option<u64>,
    pub to_line: Option<u64>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub min_severity: u8,
    /// Scope prefixes; empty for every scope.
    pub targets: Vec<String>,
    /// Text the body or an attribute contains, any case.
    pub contains: Option<String>,
    /// The most entries to return.
    pub limit: usize,
}

/// What was found.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Found {
    pub entries: Vec<Entry>,
    /// More matched than `limit`: reading forward (from a line or a time), the first `limit`
    /// were returned; otherwise the newest.
    pub truncated: bool,
    /// The oldest and newest lines kept, whatever matched (`None` when empty).
    pub first_kept: Option<u64>,
    pub last_kept: Option<u64>,
    /// Lines let go so far: every line before `first_kept`.
    pub let_go: u64,
}

#[derive(Clone, Debug)]
pub struct Ring {
    entries: VecDeque<Entry>,
    capacity: usize,
    bytes: usize,
    next_line: u64,
}

impl Ring {
    /// A ring of `capacity` lines, held within `FEWEST_LINES..=MOST_LINES`.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity: capacity.clamp(FEWEST_LINES, MOST_LINES),
            bytes: 0,
            next_line: 1,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn first(&self) -> Option<&Entry> {
        self.entries.front()
    }

    pub fn last(&self) -> Option<&Entry> {
        self.entries.back()
    }

    /// Keeps the record as the next line, letting the oldest go while the ring holds too much.
    pub fn push(&mut self, record: Record) -> u64 {
        let line = self.next_line;
        self.next_line += 1;
        let entry = cut(Entry {
            line,
            time: record.time,
            severity: record.severity,
            scope: record.scope,
            body: record.body,
            attributes: record.attributes,
        });
        self.bytes += entry.bytes();
        self.entries.push_back(entry);
        self.trim();
        line
    }

    /// Holds `capacity` lines from now on (within `FEWEST_LINES..=MOST_LINES`), letting the
    /// oldest go if it shrinks. Returns the capacity set.
    pub fn resize(&mut self, capacity: usize) -> usize {
        self.capacity = capacity.clamp(FEWEST_LINES, MOST_LINES);
        self.trim();
        self.capacity
    }

    fn trim(&mut self) {
        while self.entries.len() > self.capacity || self.bytes > MOST_BYTES {
            let Some(gone) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= gone.bytes();
        }
    }

    pub fn query(&self, query: &Query) -> Found {
        let limit = query.limit.max(1);
        let contains = query.contains.as_ref().map(|text| text.to_lowercase());
        let first_line = self
            .entries
            .front()
            .map_or(self.next_line, |entry| entry.line);
        // Lines are consecutive in the ring: a line range is a slice, found without a search.
        let start = query
            .from_line
            .map_or(0, |from| from.saturating_sub(first_line) as usize)
            .min(self.entries.len());
        let end = query.to_line.map_or(self.entries.len(), |to| {
            ((to + 1).saturating_sub(first_line) as usize).min(self.entries.len())
        });
        let matches = |entry: &&Entry| {
            entry.severity >= query.min_severity
                && query.since.is_none_or(|since| entry.time >= since)
                && query.until.is_none_or(|until| entry.time <= until)
                && (query.targets.is_empty()
                    || query
                        .targets
                        .iter()
                        .any(|prefix| entry.scope.starts_with(prefix.as_str())))
                && contains.as_ref().is_none_or(|text| {
                    entry.body.to_lowercase().contains(text)
                        || entry.attributes.iter().any(|(key, value)| {
                            key.to_lowercase().contains(text) || value.to_lowercase().contains(text)
                        })
                })
        };
        let range = self.entries.range(start..end.max(start));
        // Reading forward from a line or a time: the first matches. Otherwise the newest.
        let forward = query.from_line.is_some() || query.since.is_some();
        let (entries, truncated) = if forward {
            let mut found: Vec<Entry> = range.filter(matches).take(limit + 1).cloned().collect();
            let truncated = found.len() > limit;
            found.truncate(limit);
            (found, truncated)
        } else {
            let mut found: Vec<Entry> = range
                .rev()
                .filter(matches)
                .take(limit + 1)
                .cloned()
                .collect();
            let truncated = found.len() > limit;
            found.truncate(limit);
            found.reverse();
            (found, truncated)
        };
        Found {
            entries,
            truncated,
            first_kept: self.entries.front().map(|entry| entry.line),
            last_kept: self.entries.back().map(|entry| entry.line),
            let_go: first_line - 1,
        }
    }
}

/// A record's text cut to `MOST_RECORD_BYTES`, at a character boundary.
fn cut(mut entry: Entry) -> Entry {
    let mut room = MOST_RECORD_BYTES;
    let mut take = |text: &mut String| {
        if text.len() > room {
            let mut at = room;
            while !text.is_char_boundary(at) {
                at -= 1;
            }
            text.truncate(at);
            text.push('…');
            room = 0;
        } else {
            room -= text.len();
        }
    };
    take(&mut entry.body);
    for (_, value) in &mut entry.attributes {
        take(value);
    }
    entry
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(time: u64, severity: u8, scope: &str, body: &str) -> Record {
        Record {
            time,
            severity,
            scope: scope.into(),
            body: body.into(),
            attributes: vec![("n".into(), time.to_string())],
        }
    }

    fn lines(found: &Found) -> Vec<u64> {
        found.entries.iter().map(|entry| entry.line).collect()
    }

    #[test]
    fn lines_count_up_and_the_oldest_go_first() {
        let mut ring = Ring::new(FEWEST_LINES);
        for n in 1..=250 {
            assert_eq!(ring.push(record(n, 9, "a", "x")), n);
        }
        assert_eq!(ring.len(), FEWEST_LINES);
        let found = ring.query(&Query {
            limit: 1000,
            ..Default::default()
        });
        assert_eq!(
            (found.first_kept, found.last_kept, found.let_go),
            (Some(151), Some(250), 150)
        );
        assert_eq!(lines(&found).first(), Some(&151));
    }

    #[test]
    fn a_line_range_means_the_same_records_however_the_ring_moves() {
        let mut ring = Ring::new(1000);
        for n in 1..=500 {
            ring.push(record(n * 10, 9, "a", "x"));
        }
        let range = Query {
            from_line: Some(200),
            to_line: Some(205),
            limit: 100,
            ..Default::default()
        };
        let before = ring.query(&range);
        assert_eq!(lines(&before), [200, 201, 202, 203, 204, 205]);
        for n in 501..=700 {
            ring.push(record(n * 10, 9, "a", "x"));
        }
        assert_eq!(ring.query(&range).entries, before.entries);
        // Let go: the range finds nothing, and says where the ring begins.
        ring.resize(FEWEST_LINES);
        let gone = ring.query(&range);
        assert!(gone.entries.is_empty());
        assert_eq!(gone.first_kept, Some(601));
    }

    #[test]
    fn a_time_range_reads_forward_and_no_range_gives_the_newest() {
        let mut ring = Ring::new(1000);
        for n in 1..=100 {
            ring.push(record(n * 1000, 9, "a", "x"));
        }
        let forward = ring.query(&Query {
            since: Some(50_000),
            until: Some(60_000),
            limit: 3,
            ..Default::default()
        });
        assert_eq!(lines(&forward), [50, 51, 52]);
        assert!(forward.truncated);
        let newest = ring.query(&Query {
            limit: 3,
            ..Default::default()
        });
        assert_eq!(lines(&newest), [98, 99, 100]);
        assert!(newest.truncated);
    }

    #[test]
    fn severity_scope_and_text_narrow_it() {
        let mut ring = Ring::new(1000);
        ring.push(record(1, 5, "net::link", "connected"));
        ring.push(record(2, 13, "net::link", "Lost CONTACT"));
        ring.push(record(3, 13, "sound", "lost a frame"));
        let found = ring.query(&Query {
            min_severity: 9,
            targets: vec!["net".into()],
            contains: Some("contact".into()),
            limit: 10,
            ..Default::default()
        });
        assert_eq!(lines(&found), [2]);
    }

    #[test]
    fn the_size_is_held_within_bounds_and_long_records_are_cut() {
        let mut ring = Ring::new(5);
        assert_eq!(ring.capacity(), FEWEST_LINES);
        assert_eq!(ring.resize(10_000_000), MOST_LINES);
        ring.push(record(1, 9, "a", &"é".repeat(5000)));
        let entry = ring.last().unwrap();
        assert!(entry.body.len() <= MOST_RECORD_BYTES + '…'.len_utf8());
        assert!(entry.body.ends_with('…'));
    }
}
