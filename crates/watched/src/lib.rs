//! The watched side of the attach protocol (openspec `protocol`, `watched`): lets attachers
//! connect to this program over ZeroMQ and receive its `tracing` events as OpenTelemetry log
//! records, at the level each asks for, and its resource and status in `about`.
//!
//! ```no_run
//! use tracing_subscriber::prelude::*;
//! let watch = otel_attach_watched::Watch::start(otel_attach_watched::Config {
//!     dir: "/tmp/my-program".into(),
//!     name: "my-program".into(),
//!     version: "1.0".into(),
//!     instance: None,
//! })
//! .unwrap();
//! tracing_subscriber::registry().with(watch.layer()).init();
//! watch.set_status("role", "owner");
//! ```
//!
//! Nothing is recorded while nothing is attached: the layer's filter follows the most verbose
//! level any attacher wants, and records go to a bounded queue drained by a thread of its own,
//! never blocking the thread that logged them.

mod server;

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::InstrumentationScope;
use opentelemetry::logs::{LogRecord as _, Severity};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogProcessor, SdkLogRecord, SdkLoggerProvider};
use otel_attach_protocol::Filter;
use tokio::sync::{Notify, mpsc};
use tracing::{Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::registry::LookupSpan;

/// Records waiting to be sent, at most: past this, new ones are dropped and counted.
const QUEUE: usize = 8192;

pub struct Config {
    /// Where `attach.json` goes: the program's own directory, the user's alone.
    pub dir: PathBuf,
    /// `service.name` and `service.version`.
    pub name: String,
    pub version: String,
    /// `service.instance.id`; random if not given.
    pub instance: Option<String>,
}

/// What the program and its attach thread share.
pub(crate) struct Shared {
    /// The most verbose severity any attacher wants; 0 while none is attached.
    pub(crate) wanted: AtomicU8,
    /// Every attacher's filter, for the processor to ask before a record is built.
    pub(crate) filters: RwLock<Vec<Filter>>,
    pub(crate) status: Mutex<BTreeMap<String, String>>,
    pub(crate) status_changed: Notify,
    /// Records dropped because the queue was full, not yet reported.
    pub(crate) dropped: AtomicU64,
    pub(crate) stop: Notify,
}

/// A record and its scope, on their way to the attach thread.
pub(crate) type Queued = (SdkLogRecord, InstrumentationScope);

/// The program being watched. Dropping it says `bye` to every attacher and removes
/// `attach.json`.
pub struct Watch {
    shared: Arc<Shared>,
    provider: SdkLoggerProvider,
    endpoint: String,
    thread: Option<JoinHandle<()>>,
}

impl Watch {
    /// Binds the ROUTER socket on the loopback address and writes `attach.json`.
    pub fn start(config: Config) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            wanted: AtomicU8::new(0),
            filters: RwLock::new(Vec::new()),
            status: Mutex::new(BTreeMap::new()),
            status_changed: Notify::new(),
            dropped: AtomicU64::new(0),
            stop: Notify::new(),
        });
        let resource = resource(&config);
        let (queue, queued) = mpsc::channel(QUEUE);
        let provider = SdkLoggerProvider::builder()
            .with_resource(
                Resource::builder_empty()
                    .with_attributes(resource.iter().map(|(key, value)| {
                        opentelemetry::KeyValue::new(key.clone(), value.clone())
                    }))
                    .build(),
            )
            .with_log_processor(Processor {
                shared: Arc::clone(&shared),
                queue,
            })
            .build();
        let (endpoint, thread) = server::spawn(config.dir, resource, Arc::clone(&shared), queued)?;
        Ok(Self {
            shared,
            provider,
            endpoint,
            thread: Some(thread),
        })
    }

    /// The `tracing` layer that turns events into records for attachers. Its filter enables only
    /// what some attacher wants, so a level nobody wants costs a load and a branch.
    pub fn layer<S>(&self) -> impl Layer<S> + Send + Sync + 'static
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let shared = Arc::clone(&self.shared);
        OpenTelemetryTracingBridge::new(&self.provider).with_filter(filter_fn(move |metadata| {
            let wanted = shared.wanted.load(Ordering::Relaxed);
            wanted != 0 && level_number(metadata.level()) >= wanted
        }))
    }

    /// Sets one status pair, sent to attachers in `about` (at most every 250 ms).
    pub fn set_status(&self, key: impl Into<String>, value: impl Into<String>) {
        let (key, value) = (key.into(), value.into());
        let mut status = self
            .shared
            .status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if status.get(&key) != Some(&value) {
            status.insert(key, value);
            self.shared.status_changed.notify_one();
        }
    }

    pub fn clear_status(&self, key: &str) {
        let mut status = self
            .shared
            .status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if status.remove(key).is_some() {
            self.shared.status_changed.notify_one();
        }
    }

    /// Where it listens: `tcp://127.0.0.1:<port>`.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        self.shared.stop.notify_one();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = self.provider.shutdown();
    }
}

/// The OpenTelemetry resource: who this program is.
fn resource(config: &Config) -> BTreeMap<String, String> {
    let instance = config.instance.clone().unwrap_or_else(|| {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).expect("the OS gives random bytes");
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    });
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    BTreeMap::from([
        ("service.name".into(), config.name.clone()),
        ("service.version".into(), config.version.clone()),
        ("service.instance.id".into(), instance),
        ("process.pid".into(), std::process::id().to_string()),
        (
            "host.name".into(),
            gethostname::gethostname().to_string_lossy().into_owned(),
        ),
        ("os.type".into(), std::env::consts::OS.into()),
        (
            "process.start_time".into(),
            otel_attach_protocol::rfc3339(started),
        ),
    ])
}

/// A `tracing` level as the OpenTelemetry severity number the bridge gives it.
pub(crate) fn level_number(level: &Level) -> u8 {
    match *level {
        Level::TRACE => 1,
        Level::DEBUG => 5,
        Level::INFO => 9,
        Level::WARN => 13,
        Level::ERROR => 17,
    }
}

/// Hands each record to the attach thread, never waiting: a full queue drops it, counted.
#[derive(Debug)]
struct Processor {
    shared: Arc<Shared>,
    queue: mpsc::Sender<Queued>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Shared")
    }
}

impl LogProcessor for Processor {
    fn emit(&self, record: &mut SdkLogRecord, scope: &InstrumentationScope) {
        // When it happened: the bridge gives only the time it was observed.
        if record.timestamp().is_none() {
            record.set_timestamp(SystemTime::now());
        }
        if self
            .queue
            .try_send((record.clone(), scope.clone()))
            .is_err()
        {
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Only what some attacher wants (its severity and its targets) is built at all.
    fn event_enabled(&self, level: Severity, target: &str, _name: Option<&str>) -> bool {
        let filters = self
            .shared
            .filters
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        filters
            .iter()
            .any(|filter| filter.wants(level as u8, target))
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
}
