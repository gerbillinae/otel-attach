//! The processes the service watches (openspec `service`): each attached at its own filter, its
//! records kept in a ring buffer of its own, its name, version and status as it reports them.
//! What is watched is saved, so the service attaches again when it restarts.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use otel_attach_client::{Control, Event, Target};
use otel_attach_protocol::{About, Filter, rfc3339, severity};
use otel_attach_ring::{FEWEST_LINES, Found, MOST_LINES, Query, Record, Ring};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// A new process's ring holds this many lines unless asked otherwise.
pub const DEFAULT_LINES: usize = 10_000;

/// One watched process.
struct Monitored {
    target: Target,
    filter: Filter,
    ring: Ring,
    about: Option<About>,
    state: String,
    /// Records the program could not send, as it reported.
    dropped: u64,
    /// Dropped, it detaches.
    _control: Control,
}

/// What is saved of a watched process, to watch it again after a restart.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Saved {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub level: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<String>,
    pub lines: usize,
}

/// A process as listed.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Listed {
    /// What the other calls take as `process`.
    pub id: String,
    /// As the process reports them: `None` until it has.
    pub name: Option<String>,
    pub version: Option<String>,
    pub pid: Option<String>,
    pub host: Option<String>,
    pub instance: Option<String>,
    pub state: String,
    pub level: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<String>,
    pub buffer: Buffer,
    pub dropped_by_the_process: u64,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub status: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Buffer {
    pub capacity_lines: usize,
    pub lines_kept: usize,
    pub bytes_kept: usize,
    pub first_line: Option<u64>,
    pub last_line: Option<u64>,
    pub oldest: Option<String>,
    pub newest: Option<String>,
}

pub struct Monitors {
    processes: Mutex<BTreeMap<String, Monitored>>,
    /// Where what is watched is saved; `None` saves nothing.
    saved: Option<PathBuf>,
}

impl Monitors {
    pub fn new(saved: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            processes: Mutex::new(BTreeMap::new()),
            saved,
        })
    }

    /// Watches again what was saved. Within a tokio runtime.
    pub fn restore(self: &Arc<Self>) {
        let Some(path) = &self.saved else { return };
        let Ok(text) = std::fs::read_to_string(path) else {
            return;
        };
        let Ok(saved) = serde_json::from_str::<Vec<Saved>>(&text) else {
            eprintln!(
                "otel-attach: {} does not read: watching nothing",
                path.display()
            );
            return;
        };
        for process in saved {
            let target = match (process.dir, process.endpoint, process.token) {
                (Some(dir), _, _) => Target::Dir(dir),
                (None, Some(endpoint), Some(token)) => Target::Fixed { endpoint, token },
                _ => continue,
            };
            let filter = Filter {
                min_severity: process.level,
                targets: process.targets,
            };
            if let Err(error) = self.add(target, filter, process.lines) {
                eprintln!("otel-attach: {error}");
            }
        }
    }

    /// Starts watching `target`; refused if it is already watched. Returns its id. Within a
    /// tokio runtime.
    pub fn add(
        self: &Arc<Self>,
        target: Target,
        filter: Filter,
        lines: usize,
    ) -> Result<String, String> {
        check_lines(lines)?;
        let target = canonical(target);
        let id = target.describe();
        let mut processes = self.lock();
        if processes.contains_key(&id) {
            return Err(format!("{id} is already watched"));
        }
        let (events, mut received) = mpsc::unbounded_channel();
        let control = otel_attach_client::spawn(
            target.clone(),
            filter.clone(),
            format!("otel-attach service (pid {})", std::process::id()),
            events,
        );
        processes.insert(
            id.clone(),
            Monitored {
                target,
                filter,
                ring: Ring::new(lines),
                about: None,
                state: "looking for the program".into(),
                dropped: 0,
                _control: control,
            },
        );
        drop(processes);
        self.save();
        let monitors = Arc::clone(self);
        let key = id.clone();
        tokio::spawn(async move {
            while let Some(event) = received.recv().await {
                if !monitors.ingest(&key, event) {
                    break;
                }
            }
        });
        Ok(id)
    }

    pub fn remove(&self, process: &str) -> Result<String, String> {
        let id = self.resolve(process)?;
        self.lock().remove(&id);
        self.save();
        Ok(id)
    }

    pub fn list(&self) -> Vec<Listed> {
        self.lock()
            .iter()
            .map(|(id, monitored)| {
                let resource = monitored.about.as_ref().map(|about| &about.resource);
                let get = |key: &str| resource.and_then(|resource| resource.get(key).cloned());
                let ring = &monitored.ring;
                Listed {
                    id: id.clone(),
                    name: get("service.name"),
                    version: get("service.version"),
                    pid: get("process.pid"),
                    host: get("host.name"),
                    instance: get("service.instance.id"),
                    state: monitored.state.clone(),
                    level: severity::name(monitored.filter.min_severity).to_lowercase(),
                    targets: monitored.filter.targets.clone(),
                    buffer: Buffer {
                        capacity_lines: ring.capacity(),
                        lines_kept: ring.len(),
                        bytes_kept: ring.bytes(),
                        first_line: ring.first().map(|entry| entry.line),
                        last_line: ring.last().map(|entry| entry.line),
                        oldest: ring.first().map(|entry| rfc3339(entry.time)),
                        newest: ring.last().map(|entry| rfc3339(entry.time)),
                    },
                    dropped_by_the_process: monitored.dropped,
                    status: monitored
                        .about
                        .as_ref()
                        .map(|about| about.status.clone())
                        .unwrap_or_default(),
                }
            })
            .collect()
    }

    /// The process's records matching `query`, with its id and how it introduced itself.
    pub fn query(
        &self,
        process: &str,
        query: &Query,
    ) -> Result<(String, Option<About>, Found), String> {
        let id = self.resolve(process)?;
        let processes = self.lock();
        let monitored = &processes[&id];
        Ok((id, monitored.about.clone(), monitored.ring.query(query)))
    }

    /// Asks the process for this filter from now on.
    pub fn set_filter(&self, process: &str, filter: Filter) -> Result<String, String> {
        let id = self.resolve(process)?;
        {
            let mut processes = self.lock();
            let monitored = processes.get_mut(&id).expect("resolved");
            monitored._control.set_filter(filter.clone());
            monitored.filter = filter;
        }
        self.save();
        Ok(id)
    }

    /// Resizes the process's ring; the oldest lines go if it shrinks.
    pub fn set_lines(&self, process: &str, lines: usize) -> Result<(String, usize), String> {
        check_lines(lines)?;
        let id = self.resolve(process)?;
        let set = self
            .lock()
            .get_mut(&id)
            .expect("resolved")
            .ring
            .resize(lines);
        self.save();
        Ok((id, set))
    }

    /// The id a `process` names: an id, or the name a single watched process reports.
    fn resolve(&self, process: &str) -> Result<String, String> {
        let processes = self.lock();
        if processes.contains_key(process) {
            return Ok(process.to_string());
        }
        let expanded = expand(process);
        if processes.contains_key(&expanded) {
            return Ok(expanded);
        }
        let named: Vec<&String> = processes
            .iter()
            .filter(|(_, monitored)| {
                monitored
                    .about
                    .as_ref()
                    .and_then(|about| about.resource.get("service.name"))
                    .is_some_and(|name| name == process)
            })
            .map(|(id, _)| id)
            .collect();
        match named.as_slice() {
            [id] => Ok((*id).clone()),
            [] => Err(format!(
                "no watched process is {process}; list_processes gives the ids"
            )),
            _ => Err(format!(
                "more than one watched process is named {process}: give its id"
            )),
        }
    }

    /// An event from a process's attachment; `false` once the process is no longer watched.
    fn ingest(&self, id: &str, event: Event) -> bool {
        let mut processes = self.lock();
        let Some(monitored) = processes.get_mut(id) else {
            return false;
        };
        match event {
            Event::Waiting => monitored.state = "waiting for the program to start".into(),
            Event::Attached { endpoint } => monitored.state = format!("attached ({endpoint})"),
            Event::About(about) => monitored.about = Some(about),
            Event::Records(records) => {
                for record in records {
                    monitored.ring.push(Record {
                        time: record.time,
                        severity: record.severity,
                        scope: record.scope,
                        body: record.body,
                        attributes: record.attributes,
                    });
                }
            }
            Event::Dropped(count) => monitored.dropped += count,
            Event::Refused(reason) => monitored.state = format!("refused: {reason}"),
            Event::Again(why) => monitored.state = format!("attaching again: {why}"),
            Event::Note(_) => {}
        }
        true
    }

    fn save(&self) {
        let Some(path) = &self.saved else { return };
        let saved: Vec<Saved> = self
            .lock()
            .values()
            .map(|monitored| {
                let (dir, endpoint, token) = match &monitored.target {
                    Target::Dir(dir) => (Some(dir.clone()), None, None),
                    Target::Fixed { endpoint, token } => {
                        (None, Some(endpoint.clone()), Some(token.clone()))
                    }
                };
                Saved {
                    dir,
                    endpoint,
                    token,
                    level: monitored.filter.min_severity,
                    targets: monitored.filter.targets.clone(),
                    lines: monitored.ring.capacity(),
                }
            })
            .collect();
        if let Err(error) = write_private(
            path,
            &serde_json::to_vec_pretty(&saved).expect("serializable"),
        ) {
            eprintln!("otel-attach: cannot save {}: {error}", path.display());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Monitored>> {
        self.processes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn check_lines(lines: usize) -> Result<(), String> {
    if (FEWEST_LINES..=MOST_LINES).contains(&lines) {
        Ok(())
    } else {
        Err(format!(
            "a buffer holds {FEWEST_LINES} to {MOST_LINES} lines, not {lines}"
        ))
    }
}

/// A directory as one path, whatever way it was written (`~`, relative, a link).
fn canonical(target: Target) -> Target {
    match target {
        Target::Dir(dir) => {
            let dir = PathBuf::from(expand(&dir.to_string_lossy()));
            Target::Dir(std::fs::canonicalize(&dir).unwrap_or(dir))
        }
        fixed => fixed,
    }
}

/// `~` at the start of a path, as the home directory.
pub fn expand(path: &str) -> String {
    match (path.strip_prefix("~"), home()) {
        (Some(rest), Some(home))
            if rest.is_empty() || rest.starts_with('/') || rest.starts_with('\\') =>
        {
            format!("{}{rest}", home.display())
        }
        _ => path.to_string(),
    }
}

pub fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// Written whole, readable by the user alone where the system has modes.
pub fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let fresh = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    {
        use std::io::Write;
        options.open(&fresh)?.write_all(bytes)?;
    }
    std::fs::rename(&fresh, path)
}

/// Now, unix nanoseconds.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
