//! Attaches to a running program that speaks the attach protocol, and writes its log records to
//! stdout: one line each, or one OTLP JSON object each with `--json`. It goes on with a new copy
//! of the program when one replaces it, and detaches when interrupted.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use otel_attach_protocol::{
    About, Attach, Discovery, Filter, Message, PROTOCOL, rfc3339, severity,
};
use prost::Message as _;
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

/// How often the attacher says it is there, and how long it waits on a silent program.
const PING_EVERY: Duration = Duration::from_secs(2);
const SILENCE: Duration = Duration::from_secs(6);
/// How often it looks for the program (or a new copy of it) while it has none.
const LOOK_EVERY: Duration = Duration::from_millis(500);

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// The watched program's directory, holding its `attach.json` (also `OTEL_ATTACH_DIR`).
    #[arg(long, env = "OTEL_ATTACH_DIR")]
    dir: Option<PathBuf>,
    /// Where it listens, instead of `--dir`: `tcp://127.0.0.1:<port>`.
    #[arg(long, requires = "token", conflicts_with = "dir")]
    endpoint: Option<String>,
    /// Its token, with `--endpoint`.
    #[arg(long)]
    token: Option<String>,
    /// The least severe records wanted: trace, debug, info, warn, error, fatal.
    #[arg(long, default_value = "info", value_parser = parse_level)]
    level: u8,
    /// Only records from scopes starting with this (a Rust module path); repeatable.
    #[arg(long = "target")]
    targets: Vec<String>,
    /// One OTLP JSON object per record, and `about` as JSON, instead of lines.
    #[arg(long)]
    json: bool,
}

fn parse_level(name: &str) -> Result<u8, String> {
    severity::parse(name).ok_or_else(|| format!("not a level: {name}"))
}

/// Where to attach, and how to find it again.
enum Target {
    Dir(PathBuf),
    Fixed(Discovery),
}

impl Target {
    fn discovery(&self) -> Option<Discovery> {
        match self {
            Target::Dir(dir) => Discovery::read(dir).ok(),
            Target::Fixed(discovery) => Some(discovery.clone()),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = Args::parse();
    let target = match (&args.dir, &args.endpoint, &args.token) {
        (Some(dir), _, _) => Target::Dir(dir.clone()),
        (None, Some(endpoint), Some(token)) => Target::Fixed(Discovery {
            protocol: PROTOCOL,
            endpoint: endpoint.clone(),
            token: token.clone(),
            pid: 0,
        }),
        _ => {
            eprintln!("otel-attach: give --dir (or OTEL_ATTACH_DIR), or --endpoint and --token");
            return ExitCode::FAILURE;
        }
    };
    let filter = Filter {
        min_severity: args.level,
        targets: args.targets.clone(),
    };
    let out = Output {
        json: args.json,
        introduced: std::cell::RefCell::new(None),
    };
    let mut last: Option<Discovery> = None;
    loop {
        let Some(discovery) = wait_for(&target, last.as_ref()).await else {
            return ExitCode::SUCCESS;
        };
        match session(&discovery, &target, &filter, &out).await {
            Ended::Interrupted => return ExitCode::SUCCESS,
            Ended::Refused(reason) => {
                eprintln!("otel-attach: refused: {reason}");
                if matches!(target, Target::Fixed(_)) {
                    return ExitCode::FAILURE;
                }
            }
            Ended::Again(note) => out.note(&note),
        }
        last = Some(discovery);
    }
}

/// The program to attach to: the first found, or, after a session, one that is not the last
/// (a new copy), or the last again once it answers. `None` when interrupted while waiting.
async fn wait_for(target: &Target, last: Option<&Discovery>) -> Option<Discovery> {
    let mut said = false;
    loop {
        if let Some(discovery) = target.discovery() {
            let fresh = last.is_none_or(|last| *last != discovery);
            if fresh || matches!(target, Target::Fixed(_)) || answers(&discovery.endpoint).await {
                return Some(discovery);
            }
        }
        if !said {
            if let Target::Dir(dir) = target {
                eprintln!(
                    "# waiting for the program ({})",
                    Discovery::path(dir).display()
                );
            }
            said = true;
        }
        tokio::select! {
            _ = tokio::time::sleep(LOOK_EVERY) => {}
            _ = tokio::signal::ctrl_c() => return None,
        }
    }
}

/// Whether something listens at the endpoint.
async fn answers(endpoint: &str) -> bool {
    let address = endpoint.trim_start_matches("tcp://");
    tokio::net::TcpStream::connect(address).await.is_ok()
}

enum Ended {
    Interrupted,
    Refused(String),
    /// Attach again, after saying this.
    Again(String),
}

/// One attachment: from `attach` until the program goes, is replaced, falls silent, or the user
/// interrupts.
async fn session(discovery: &Discovery, target: &Target, filter: &Filter, out: &Output) -> Ended {
    let mut socket = DealerSocket::new();
    if let Err(error) = socket.connect(&discovery.endpoint).await {
        tokio::time::sleep(LOOK_EVERY).await;
        return Ended::Again(format!("cannot connect to {}: {error}", discovery.endpoint));
    }
    let attach = Message::Attach(Attach {
        protocol: PROTOCOL,
        token: discovery.token.clone(),
        filter: filter.clone(),
        name: Some(format!("otel-attach (pid {})", std::process::id())),
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
                match Message::decode(kind, body) {
                    Ok(Message::Welcome { .. }) => out.note(&format!("attached to {}", discovery.endpoint)),
                    Ok(Message::Refused { reason, .. }) => return Ended::Refused(reason),
                    Ok(Message::About(about)) => out.about(&about),
                    Ok(Message::Logs(bytes)) => match ExportLogsServiceRequest::decode(bytes.as_slice()) {
                        Ok(request) => out.logs(&request),
                        Err(error) => out.note(&format!("records that do not decode: {error}")),
                    },
                    Ok(Message::Dropped { count }) => out.note(&format!("{count} records dropped")),
                    Ok(Message::Bye { reason }) => {
                        return Ended::Again(format!("the program said bye: {reason}"));
                    }
                    Ok(_) => {}
                    Err(error) => out.note(&format!("a message that does not decode: {error}")),
                }
            }
            _ = ticks.tick() => {
                if heard.elapsed() >= SILENCE {
                    return Ended::Again("nothing from the program for 6 s".into());
                }
                if let Target::Dir(dir) = target
                    && replaced(dir, discovery)
                {
                    return Ended::Again("a new copy of the program started".into());
                }
                if pinged.elapsed() >= PING_EVERY {
                    pinged = Instant::now();
                    let _ = send(&mut socket, &Message::Ping).await;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                let _ = send(&mut socket, &Message::Detach).await;
                // A send only queues: a moment for it to leave.
                tokio::time::sleep(Duration::from_millis(100)).await;
                return Ended::Interrupted;
            }
        }
    }
}

/// `attach.json` now names another copy.
fn replaced(dir: &Path, attached: &Discovery) -> bool {
    Discovery::read(dir).is_ok_and(|now| now != *attached)
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

struct Output {
    json: bool,
    /// The program last introduced, so that only its status is printed again.
    introduced: std::cell::RefCell<Option<std::collections::BTreeMap<String, String>>>,
}

impl Output {
    /// Something about the attachment, not from the program.
    fn note(&self, text: &str) {
        if self.json {
            println!("{}", serde_json::json!({ "type": "note", "text": text }));
        } else {
            println!("# {text}");
        }
    }

    fn about(&self, about: &About) {
        if self.json {
            println!(
                "{}",
                serde_json::json!({ "type": "about", "resource": about.resource, "status": about.status })
            );
            return;
        }
        let get = |key: &str| about.resource.get(key).map(String::as_str).unwrap_or("?");
        let known = self.introduced.borrow().as_ref() == Some(&about.resource);
        if !known {
            *self.introduced.borrow_mut() = Some(about.resource.clone());
            println!(
                "# {} {} on {} (pid {}, instance {})",
                get("service.name"),
                get("service.version"),
                get("host.name"),
                get("process.pid"),
                get("service.instance.id")
            );
        }
        if !about.status.is_empty() {
            let status: Vec<String> = about
                .status
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect();
            println!("# status: {}", status.join(" "));
        }
    }

    fn logs(&self, request: &ExportLogsServiceRequest) {
        for resource in &request.resource_logs {
            for scope in &resource.scope_logs {
                let name = scope.scope.as_ref().map_or("", |scope| scope.name.as_str());
                for record in &scope.log_records {
                    if self.json {
                        self.json_record(name, record);
                    } else {
                        println!("{}", line(name, record));
                    }
                }
            }
        }
    }

    fn json_record(&self, scope: &str, record: &LogRecord) {
        let mut value = serde_json::to_value(record).unwrap_or_default();
        if let Some(object) = value.as_object_mut() {
            object.insert("scope".into(), serde_json::Value::String(scope.into()));
        }
        println!("{value}");
    }
}

/// `2026-11-11T15:02:03.123Z INFO  pointer_share::service  role changed  role=owner`
fn line(scope: &str, record: &LogRecord) -> String {
    let time = if record.time_unix_nano != 0 {
        record.time_unix_nano
    } else {
        record.observed_time_unix_nano
    };
    let severity = u8::try_from(record.severity_number).unwrap_or(0);
    let body = record.body.as_ref().map(show).unwrap_or_default();
    let mut line = format!(
        "{} {:<5} {scope}  {body}",
        rfc3339(time),
        severity::name(severity)
    );
    for KeyValue { key, value, .. } in &record.attributes {
        let value = value.as_ref().map(show).unwrap_or_default();
        line.push_str(&format!("  {key}={value}"));
    }
    line
}

fn show(value: &AnyValue) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_is_one_line() {
        let record = LogRecord {
            time_unix_nano: 1_794_409_323_123_000_000,
            severity_number: 9,
            body: Some(AnyValue {
                value: Some(Value::StringValue("role changed".into())),
            }),
            attributes: vec![KeyValue {
                key: "role".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("owner".into())),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            line("pointer_share::service", &record),
            "2026-11-11T15:02:03.123Z INFO  pointer_share::service  role changed  role=owner"
        );
    }
}
