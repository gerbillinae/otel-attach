//! The attach thread: the ROUTER socket, each attacher's filter and lease, and records batched
//! into OTLP requests for those who want them.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use otel_attach_protocol::{About, Discovery, Filter, Message, PROTOCOL};
use prost::Message as _;
use tokio::sync::mpsc;
use zeromq::{
    RouterRecvHalf, RouterSendHalf, RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage,
};

use crate::{Queued, Shared};

/// Records go out at least this often, or in batches of this many, whichever comes first.
const BATCH_EVERY: Duration = Duration::from_millis(100);
const BATCH_MOST: usize = 256;
/// An attacher not heard from this long is forgotten (ROUTER does not say when a peer goes).
const LEASE: Duration = Duration::from_secs(6);
/// `about` goes again at most this often while the status changes.
const ABOUT_EVERY: Duration = Duration::from_millis(250);
/// A send that waits longer than this (an attacher not reading) is given up, its records counted
/// as dropped: the program never waits on an attacher.
const SEND_WAIT: Duration = Duration::from_millis(100);
/// How long the last messages are given to leave before the socket closes.
const LINGER: Duration = Duration::from_millis(200);

/// Starts the attach thread; returns once it listens and `attach.json` is written.
pub(crate) fn spawn(
    dir: PathBuf,
    resource: BTreeMap<String, String>,
    shared: Arc<Shared>,
    queued: mpsc::Receiver<Queued>,
) -> io::Result<(String, JoinHandle<()>)> {
    let (ready, listening) = std::sync::mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("attach".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready.send(Err(error.to_string()));
                    return;
                }
            };
            runtime.block_on(run(dir, resource, shared, queued, ready));
        })?;
    match listening.recv() {
        Ok(Ok(endpoint)) => Ok((endpoint, thread)),
        Ok(Err(error)) => Err(io::Error::other(error)),
        Err(_) => Err(io::Error::other("the attach thread stopped while starting")),
    }
}

struct Attacher {
    filter: Filter,
    heard: Instant,
    /// Records waiting to go, with their scopes.
    batch: Vec<(String, LogRecord)>,
    /// Records it did not get, not yet reported.
    dropped: u64,
}

struct Server {
    attachers: HashMap<Bytes, Attacher>,
    resource: BTreeMap<String, String>,
    token: String,
    shared: Arc<Shared>,
    about_due: bool,
    about_sent: Option<Instant>,
    send: RouterSendHalf,
}

async fn run(
    dir: PathBuf,
    resource: BTreeMap<String, String>,
    shared: Arc<Shared>,
    mut queued: mpsc::Receiver<Queued>,
    ready: std::sync::mpsc::Sender<Result<String, String>>,
) {
    let mut socket = RouterSocket::new();
    let endpoint = match socket.bind("tcp://127.0.0.1:0").await {
        Ok(endpoint) => endpoint.to_string(),
        Err(error) => {
            let _ = ready.send(Err(format!("cannot listen for attachers: {error}")));
            return;
        }
    };
    let mut token = [0u8; 32];
    getrandom::fill(&mut token).expect("the OS gives random bytes");
    let token: String = token.iter().map(|byte| format!("{byte:02x}")).collect();
    let pid = std::process::id();
    let discovery = Discovery {
        protocol: PROTOCOL,
        endpoint: endpoint.clone(),
        token: token.clone(),
        pid,
    };
    if let Err(error) = discovery.write(&dir) {
        let _ = ready.send(Err(format!(
            "cannot write {}: {error}",
            Discovery::path(&dir).display()
        )));
        return;
    }
    let _ = ready.send(Ok(endpoint));
    let (send, mut recv): (RouterSendHalf, RouterRecvHalf) = socket.split();
    let mut server = Server {
        attachers: HashMap::new(),
        resource,
        token,
        shared: Arc::clone(&shared),
        about_due: false,
        about_sent: None,
        send,
    };
    let mut tick = tokio::time::interval(BATCH_EVERY);
    loop {
        tokio::select! {
            received = recv.recv() => {
                if let Ok(message) = received {
                    server.control(message).await;
                }
            }
            Some((record, scope)) = queued.recv() => server.record(&record, &scope).await,
            _ = tick.tick() => server.tick().await,
            _ = shared.status_changed.notified() => server.about_due = true,
            _ = shared.stop.notified() => {
                server.bye("the program is exiting").await;
                break;
            }
        }
    }
    Discovery::remove_if_ours(&dir, pid);
}

impl Server {
    /// A message from an attacher: its identity, then the type and the body.
    async fn control(&mut self, message: ZmqMessage) {
        let frames = message.into_vec();
        let [identity, kind, body] = frames.as_slice() else {
            return;
        };
        let Ok(message) = Message::decode(kind, body) else {
            return;
        };
        match message {
            Message::Attach(attach) => {
                let refusal = if attach.protocol != PROTOCOL {
                    Some(format!(
                        "protocol {} is not spoken here (only {PROTOCOL})",
                        attach.protocol
                    ))
                } else if attach.token != self.token {
                    Some("wrong token".to_string())
                } else {
                    None
                };
                if let Some(reason) = refusal {
                    self.send_to(
                        identity,
                        &Message::Refused {
                            reason,
                            protocol: PROTOCOL,
                        },
                    )
                    .await;
                    return;
                }
                self.attachers.insert(
                    identity.clone(),
                    Attacher {
                        filter: attach.filter,
                        heard: Instant::now(),
                        batch: Vec::new(),
                        dropped: 0,
                    },
                );
                self.wanted();
                self.send_to(identity, &Message::Welcome { protocol: PROTOCOL })
                    .await;
                let about = Message::About(self.about());
                self.send_to(identity, &about).await;
            }
            Message::Level(filter) => {
                if let Some(attacher) = self.attachers.get_mut(identity) {
                    attacher.filter = filter;
                    attacher.heard = Instant::now();
                    self.wanted();
                }
            }
            Message::Ping => {
                if let Some(attacher) = self.attachers.get_mut(identity) {
                    attacher.heard = Instant::now();
                    self.send_to(identity, &Message::Pong).await;
                }
            }
            Message::Detach if self.attachers.remove(identity).is_some() => self.wanted(),
            // Out of turn, or from a newer version: ignored.
            _ => {}
        }
    }

    /// A record, for every attacher that wants it.
    async fn record(
        &mut self,
        record: &opentelemetry_sdk::logs::SdkLogRecord,
        scope: &opentelemetry::InstrumentationScope,
    ) {
        let severity = record
            .severity_number()
            .map_or(0, |severity| severity as u8);
        let target = record
            .target()
            .map(|target| target.to_string())
            .unwrap_or_else(|| scope.name().to_string());
        let mut converted: Option<LogRecord> = None;
        let mut full = Vec::new();
        for (identity, attacher) in &mut self.attachers {
            if !attacher.filter.wants(severity, &target) {
                continue;
            }
            let proto = converted
                .get_or_insert_with(|| LogRecord::from(record))
                .clone();
            attacher.batch.push((target.clone(), proto));
            if attacher.batch.len() >= BATCH_MOST {
                full.push(identity.clone());
            }
        }
        for identity in full {
            self.flush(&identity).await;
        }
    }

    /// Batches due, leases run out, drops and status reported.
    async fn tick(&mut self) {
        let now = Instant::now();
        let before = self.attachers.len();
        self.attachers
            .retain(|_, attacher| now.duration_since(attacher.heard) < LEASE);
        if self.attachers.len() != before {
            self.wanted();
        }
        let dropped = self.shared.dropped.swap(0, Ordering::Relaxed);
        let identities: Vec<Bytes> = self.attachers.keys().cloned().collect();
        for identity in &identities {
            self.flush(identity).await;
            let Some(attacher) = self.attachers.get_mut(identity) else {
                continue;
            };
            attacher.dropped += dropped;
            if attacher.dropped > 0 {
                let count = std::mem::take(&mut attacher.dropped);
                self.send_to(identity, &Message::Dropped { count }).await;
            }
        }
        if self.about_due
            && self
                .about_sent
                .is_none_or(|sent| now.duration_since(sent) >= ABOUT_EVERY)
        {
            self.about_due = false;
            self.about_sent = Some(now);
            let about = Message::About(self.about());
            for identity in &identities {
                self.send_to(identity, &about).await;
            }
        }
    }

    /// The attacher's waiting records, as one OTLP request: one resource, a scope per target.
    async fn flush(&mut self, identity: &Bytes) {
        let Some(attacher) = self.attachers.get_mut(identity) else {
            return;
        };
        if attacher.batch.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut attacher.batch);
        let count = batch.len() as u64;
        let mut scopes: Vec<ScopeLogs> = Vec::new();
        for (target, record) in batch {
            match scopes.iter_mut().find(|scope| {
                scope
                    .scope
                    .as_ref()
                    .is_some_and(|scope| scope.name == target)
            }) {
                Some(scope) => scope.log_records.push(record),
                None => scopes.push(ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: target,
                        ..Default::default()
                    }),
                    log_records: vec![record],
                    ..Default::default()
                }),
            }
        }
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: self
                        .resource
                        .iter()
                        .map(|(key, value)| KeyValue {
                            key: key.clone(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(value.clone())),
                            }),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }),
                scope_logs: scopes,
                ..Default::default()
            }],
        };
        if !self
            .send_to(identity, &Message::Logs(request.encode_to_vec()))
            .await
            && let Some(attacher) = self.attachers.get_mut(identity)
        {
            attacher.dropped += count;
        }
    }

    /// Says `bye` to every attacher, and gives the messages a moment to leave: a send only queues
    /// them, and the socket goes with the thread.
    async fn bye(&mut self, reason: &str) {
        let identities: Vec<Bytes> = self.attachers.keys().cloned().collect();
        for identity in &identities {
            let identity = identity.clone();
            self.flush(&identity).await;
            self.send_to(
                &identity,
                &Message::Bye {
                    reason: reason.to_string(),
                },
            )
            .await;
        }
        if !identities.is_empty() {
            tokio::time::sleep(LINGER).await;
        }
    }

    fn about(&self) -> About {
        About {
            resource: self.resource.clone(),
            status: self
                .shared
                .status
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
        }
    }

    /// The most verbose level any attacher wants, and every filter, for the program's threads.
    fn wanted(&self) {
        let wanted = self
            .attachers
            .values()
            .map(|attacher| attacher.filter.min_severity.max(1))
            .min()
            .unwrap_or(0);
        *self
            .shared
            .filters
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = self
            .attachers
            .values()
            .map(|attacher| attacher.filter.clone())
            .collect();
        // `tracing` caches, per call site, whether a layer wants it, the first time it runs: what
        // was off before anyone attached must be asked again.
        if self.shared.wanted.swap(wanted, Ordering::Relaxed) != wanted {
            tracing::callsite::rebuild_interest_cache();
        }
    }

    /// Whether the message left; never waits longer than `SEND_WAIT`.
    async fn send_to(&mut self, identity: &Bytes, message: &Message) -> bool {
        let [kind, body] = message.encode();
        let mut frames = ZmqMessage::from(identity.clone());
        frames.push_back(Bytes::from(kind));
        frames.push_back(Bytes::from(body));
        matches!(
            tokio::time::timeout(SEND_WAIT, self.send.send(frames)).await,
            Ok(Ok(()))
        )
    }
}
