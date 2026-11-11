//! A watched program and an attacher over real ZeroMQ sockets on the loopback address.

use std::time::Duration;

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use otel_attach_protocol::{Attach, Discovery, Filter, Message, PROTOCOL, severity};
use otel_attach_watched::{Config, Watch};
use prost::Message as _;
use tracing_subscriber::prelude::*;
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

fn frames(message: &Message) -> ZmqMessage {
    let [kind, body] = message.encode();
    let mut frames = ZmqMessage::from(Bytes::from(kind));
    frames.push_back(Bytes::from(body));
    frames
}

/// The next message, within two seconds.
async fn next(socket: &mut DealerSocket) -> Message {
    let received = tokio::time::timeout(Duration::from_secs(2), socket.recv())
        .await
        .expect("a message within two seconds")
        .expect("received");
    let frames = received.into_vec();
    Message::decode(&frames[0], &frames[1]).expect("decodes")
}

/// The next message that is not `about` or `dropped`.
async fn next_logs(socket: &mut DealerSocket) -> ExportLogsServiceRequest {
    loop {
        if let Message::Logs(bytes) = next(socket).await {
            return ExportLogsServiceRequest::decode(bytes.as_slice()).expect("OTLP");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_attacher_gets_what_it_asks_for_and_hears_the_program_go() {
    let dir = tempdir();
    let watch = Watch::start(Config {
        dir: dir.clone(),
        name: "watched-test".into(),
        version: "1.2.3".into(),
        instance: Some("instance-1".into()),
    })
    .unwrap();
    let _logging =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(watch.layer()));
    let discovery = Discovery::read(&dir).unwrap();
    assert_eq!(discovery.endpoint, watch.endpoint());

    // The wrong token is refused.
    let mut stranger = DealerSocket::new();
    stranger.connect(&discovery.endpoint).await.unwrap();
    stranger
        .send(frames(&Message::Attach(Attach {
            protocol: PROTOCOL,
            token: "guess".into(),
            filter: Filter::default(),
            name: None,
        })))
        .await
        .unwrap();
    assert!(matches!(next(&mut stranger).await, Message::Refused { .. }));

    // The right one: welcome, then who it is.
    let mut socket = DealerSocket::new();
    socket.connect(&discovery.endpoint).await.unwrap();
    socket
        .send(frames(&Message::Attach(Attach {
            protocol: PROTOCOL,
            token: discovery.token.clone(),
            filter: Filter {
                min_severity: severity::INFO,
                targets: Vec::new(),
            },
            name: Some("test".into()),
        })))
        .await
        .unwrap();
    assert_eq!(
        next(&mut socket).await,
        Message::Welcome { protocol: PROTOCOL }
    );
    let Message::About(about) = next(&mut socket).await else {
        panic!("about after welcome")
    };
    assert_eq!(about.resource["service.name"], "watched-test");
    assert_eq!(about.resource["service.version"], "1.2.3");
    assert_eq!(about.resource["service.instance.id"], "instance-1");

    // A debug event is not wanted; an info one comes as a structured record.
    tracing::debug!(target: "test::quiet", "not sent");
    tracing::info!(target: "test::loud", answer = 42, "hello {}", "there");
    let request = next_logs(&mut socket).await;
    let resource = request.resource_logs[0].resource.as_ref().unwrap();
    assert!(
        resource
            .attributes
            .iter()
            .any(|attribute| attribute.key == "service.name")
    );
    let scopes = &request.resource_logs[0].scope_logs;
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[0].scope.as_ref().unwrap().name, "test::loud");
    let record = &scopes[0].log_records[0];
    assert_eq!(record.severity_number, 9);
    assert_eq!(
        record.body.as_ref().and_then(|body| body.value.clone()),
        Some(Value::StringValue("hello there".into()))
    );
    assert!(record.attributes.iter().any(|attribute| {
        attribute.key == "answer"
            && attribute
                .value
                .as_ref()
                .and_then(|value| value.value.clone())
                == Some(Value::IntValue(42))
    }));

    // A status change sends `about` again.
    watch.set_status("role", "owner");
    loop {
        if let Message::About(about) = next(&mut socket).await {
            assert_eq!(about.status["role"], "owner");
            break;
        }
    }

    // Going: bye, and the discovery file removed.
    drop(_logging);
    drop(watch);
    loop {
        if let Message::Bye { .. } = next(&mut socket).await {
            break;
        }
    }
    assert!(!Discovery::path(&dir).exists());
    let _ = std::fs::remove_dir_all(&dir);
}

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "otel-attach-watched-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
