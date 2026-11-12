//! The attacher's side of the attach protocol (openspec `protocol`, `attacher`): finds a watched
//! program, attaches at a filter, and reports what comes as events, attaching again when the
//! program says goodbye, falls silent, or is replaced by a new copy. One task per program; its
//! filter can be changed while it runs.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value::Value};
use otel_attach_protocol::{About, Attach, Discovery, Filter, Message, PROTOCOL};
use prost::Message as _;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

/// How often the attacher says it is there, and how long it waits on a silent program.
const PING_EVERY: Duration = Duration::from_secs(2);
const SILENCE: Duration = Duration::from_secs(6);
/// How often it looks for the program (or a new copy of it) while it has none.
const LOOK_EVERY: Duration = Duration::from_millis(500);

/// Where to attach, and how to find it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// The program's directory, holding its `attach.json`: followed across new copies.
    Dir(PathBuf),
    /// An endpoint and token given outright.
    Fixed { endpoint: String, token: String },
}

impl Target {
    fn discovery(&self) -> Option<Discovery> {
        match self {
            Target::Dir(dir) => Discovery::read(dir).ok(),
            Target::Fixed { endpoint, token } => Some(Discovery {
                protocol: PROTOCOL,
                endpoint: endpoint.clone(),
                token: token.clone(),
                pid: 0,
            }),
        }
    }

    /// For people: the directory or the endpoint.
    pub fn describe(&self) -> String {
        match self {
            Target::Dir(dir) => dir.display().to_string(),
            Target::Fixed { endpoint, .. } => endpoint.clone(),
        }
    }
}

/// What happens to an attachment.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// No program found yet (or after it went): waiting for one.
    Waiting,
    Attached {
        endpoint: String,
    },
    About(About),
    Records(Vec<Record>),
    Dropped(u64),
    Refused(String),
    /// Attaching again, and why.
    Again(String),
    /// Something not from the program (a message that did not decode).
    Note(String),
}

/// A record as received, flattened from OTLP.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    /// When it happened (or was observed), unix nanoseconds.
    pub time: u64,
    pub severity: u8,
    pub scope: String,
    pub body: String,
    pub attributes: Vec<(String, String)>,
    /// The OTLP record as it came, for output that wants all of it.
    pub otlp: opentelemetry_proto::tonic::logs::v1::LogRecord,
}

/// Every record in an OTLP request, flattened.
pub fn records(request: &ExportLogsServiceRequest) -> Vec<Record> {
    let mut records = Vec::new();
    for resource in &request.resource_logs {
        for scope in &resource.scope_logs {
            let name = scope.scope.as_ref().map_or("", |scope| scope.name.as_str());
            for record in &scope.log_records {
                records.push(Record {
                    time: if record.time_unix_nano != 0 {
                        record.time_unix_nano
                    } else {
                        record.observed_time_unix_nano
                    },
                    severity: u8::try_from(record.severity_number).unwrap_or(0),
                    scope: name.to_string(),
                    body: record.body.as_ref().map(show).unwrap_or_default(),
                    attributes: record
                        .attributes
                        .iter()
                        .map(|pair| {
                            (
                                pair.key.clone(),
                                pair.value.as_ref().map(show).unwrap_or_default(),
                            )
                        })
                        .collect(),
                    otlp: record.clone(),
                });
            }
        }
    }
    records
}

/// An OTLP value as text.
pub fn show(value: &AnyValue) -> String {
    match &value.value {
        Some(Value::StringValue(text)) => text.clone(),
        Some(Value::BoolValue(flag)) => flag.to_string(),
        Some(Value::IntValue(number)) => number.to_string(),
        Some(Value::DoubleValue(number)) => number.to_string(),
        Some(Value::BytesValue(bytes)) => format!("<{} bytes>", bytes.len()),
        Some(Value::ArrayValue(array)) => format!(
            "[{}]",
            array.values.iter().map(show).collect::<Vec<_>>().join(", ")
        ),
        Some(Value::KvlistValue(list)) => format!(
            "{{{}}}",
            list.values
                .iter()
                .map(|pair| format!(
                    "{}: {}",
                    pair.key,
                    pair.value.as_ref().map(show).unwrap_or_default()
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Some(other) => format!("{other:?}"),
        None => String::new(),
    }
}

/// Changes an attachment while it runs. Dropping it detaches and ends the task.
pub struct Control {
    commands: mpsc::UnboundedSender<Command>,
    task: Option<JoinHandle<()>>,
}

enum Command {
    Filter(Filter),
    Stop,
}

impl Control {
    /// Asks for this filter from now on (and on every later attach).
    pub fn set_filter(&self, filter: Filter) {
        let _ = self.commands.send(Command::Filter(filter));
    }

    /// Detaches, and waits for the task to end.
    pub async fn stop(mut self) {
        let _ = self.commands.send(Command::Stop);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for Control {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Stop);
    }
}

/// Starts attaching to `target` at `filter`, as `name`; what happens comes on `events`. Must be
/// called within a tokio runtime.
pub fn spawn(
    target: Target,
    filter: Filter,
    name: String,
    events: mpsc::UnboundedSender<Event>,
) -> Control {
    let (commands, received) = mpsc::unbounded_channel();
    let task = tokio::spawn(run(target, filter, name, events, received));
    Control {
        commands,
        task: Some(task),
    }
}

async fn run(
    target: Target,
    mut filter: Filter,
    name: String,
    events: mpsc::UnboundedSender<Event>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let mut last: Option<Discovery> = None;
    loop {
        let Some(discovery) =
            wait_for(&target, last.as_ref(), &events, &mut commands, &mut filter).await
        else {
            return;
        };
        match session(
            &discovery,
            &target,
            &mut filter,
            &name,
            &events,
            &mut commands,
        )
        .await
        {
            Ended::Stopped => return,
            Ended::Refused(reason) => {
                let _ = events.send(Event::Refused(reason));
                if matches!(target, Target::Fixed { .. }) {
                    return;
                }
            }
            Ended::Again(why) => {
                let _ = events.send(Event::Again(why));
            }
        }
        last = Some(discovery);
    }
}

/// The program to attach to: the first found, or, after a session, one that is not the last (a
/// new copy), or the last again once it answers. `None` when stopped meanwhile.
async fn wait_for(
    target: &Target,
    last: Option<&Discovery>,
    events: &mpsc::UnboundedSender<Event>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    filter: &mut Filter,
) -> Option<Discovery> {
    let mut said = false;
    loop {
        if let Some(discovery) = target.discovery() {
            let fresh = last.is_none_or(|last| *last != discovery);
            if fresh || matches!(target, Target::Fixed { .. }) || answers(&discovery.endpoint).await
            {
                return Some(discovery);
            }
        }
        if !said {
            let _ = events.send(Event::Waiting);
            said = true;
        }
        tokio::select! {
            _ = tokio::time::sleep(LOOK_EVERY) => {}
            command = commands.recv() => match command {
                Some(Command::Filter(wanted)) => *filter = wanted,
                Some(Command::Stop) | None => return None,
            },
        }
    }
}

/// Whether something listens at the endpoint.
async fn answers(endpoint: &str) -> bool {
    let address = endpoint.trim_start_matches("tcp://");
    tokio::net::TcpStream::connect(address).await.is_ok()
}

enum Ended {
    Stopped,
    Refused(String),
    Again(String),
}

/// One attachment: from `attach` until the program goes, is replaced, falls silent, or it is
/// stopped.
async fn session(
    discovery: &Discovery,
    target: &Target,
    filter: &mut Filter,
    name: &str,
    events: &mpsc::UnboundedSender<Event>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
) -> Ended {
    let mut socket = DealerSocket::new();
    if let Err(error) = socket.connect(&discovery.endpoint).await {
        tokio::time::sleep(LOOK_EVERY).await;
        return Ended::Again(format!("cannot connect to {}: {error}", discovery.endpoint));
    }
    let attach = Message::Attach(Attach {
        protocol: PROTOCOL,
        token: discovery.token.clone(),
        filter: filter.clone(),
        name: Some(name.to_string()),
    });
    if send(&mut socket, &attach).await.is_err() {
        return Ended::Again("could not attach".into());
    }
    let mut heard = Instant::now();
    let mut pinged = Instant::now();
    let mut ticks = tokio::time::interval(LOOK_EVERY);
    loop {
        tokio::select! {
            received = socket.recv() => {
                let Ok(received) = received else {
                    return Ended::Again("the connection closed".into());
                };
                heard = Instant::now();
                let frames = received.into_vec();
                let [kind, body] = frames.as_slice() else { continue };
                let event = match Message::decode(kind, body) {
                    Ok(Message::Welcome { .. }) => Event::Attached { endpoint: discovery.endpoint.clone() },
                    Ok(Message::Refused { reason, .. }) => return Ended::Refused(reason),
                    Ok(Message::About(about)) => Event::About(about),
                    Ok(Message::Logs(bytes)) => match ExportLogsServiceRequest::decode(bytes.as_slice()) {
                        Ok(request) => Event::Records(records(&request)),
                        Err(error) => Event::Note(format!("records that do not decode: {error}")),
                    },
                    Ok(Message::Dropped { count }) => Event::Dropped(count),
                    Ok(Message::Bye { reason }) => return Ended::Again(format!("the program said bye: {reason}")),
                    Ok(_) => continue,
                    Err(error) => Event::Note(format!("a message that does not decode: {error}")),
                };
                let _ = events.send(event);
            }
            command = commands.recv() => match command {
                Some(Command::Filter(wanted)) => {
                    *filter = wanted;
                    let _ = send(&mut socket, &Message::Level(filter.clone())).await;
                }
                Some(Command::Stop) | None => {
                    let _ = send(&mut socket, &Message::Detach).await;
                    // A send only queues: a moment for it to leave.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    return Ended::Stopped;
                }
            },
            _ = ticks.tick() => {
                if heard.elapsed() >= SILENCE {
                    return Ended::Again("nothing from the program for 6 s".into());
                }
                if let Target::Dir(dir) = target
                    && Discovery::read(dir).is_ok_and(|now| now != *discovery)
                {
                    return Ended::Again("a new copy of the program started".into());
                }
                if pinged.elapsed() >= PING_EVERY {
                    pinged = Instant::now();
                    let _ = send(&mut socket, &Message::Ping).await;
                }
            }
        }
    }
}

async fn send(socket: &mut DealerSocket, message: &Message) -> Result<(), String> {
    let [kind, body] = message.encode();
    let mut frames = ZmqMessage::from(Bytes::from(kind));
    frames.push_back(Bytes::from(body));
    tokio::time::timeout(Duration::from_secs(1), socket.send(frames))
        .await
        .map_err(|_| "timed out".to_string())?
        .map_err(|error| error.to_string())
}
