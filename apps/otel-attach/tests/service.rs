//! The service over real HTTP, with a real MCP client, watching a real watched program in this
//! process.

use std::time::Duration;

use otel_attach::mcp;
use otel_attach::monitors::Monitors;
use otel_attach_watched::{Config, Watch};
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, CallToolResult, ClientConfig};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::prelude::*;

const TOKEN: &str = "test-token";

fn text(result: &CallToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn call(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    tool: &'static str,
    arguments: Value,
) -> (bool, String) {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool)
                .with_arguments(arguments.as_object().cloned().unwrap_or_default()),
        )
        .await
        .expect("the call goes through");
    (result.is_error != Some(true), text(&result))
}

fn tempdir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "otel-attach-service-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_watches_a_program_and_reads_its_log_back() {
    let program = tempdir("program");
    let state = tempdir("state");
    let watch = Watch::start(Config {
        dir: program.clone(),
        name: "watched-test".into(),
        version: "4.5.6".into(),
        instance: None,
    })
    .unwrap();
    let _logging =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(watch.layer()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let monitors = Monitors::new(Some(state.join("processes.json")));
    let server = tokio::spawn(mcp::serve(listener, monitors, TOKEN.into(), stop.clone()));

    // Without the token, nothing.
    let stranger = ClientConfig::default()
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp")),
        ))
        .await;
    assert!(stranger.is_err(), "refused without the token");

    let client = ClientConfig::default()
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{address}/mcp"))
                .auth_header(TOKEN),
        ))
        .await
        .expect("the client starts");
    let tools: Vec<String> = client
        .list_tools(None)
        .await
        .unwrap()
        .tools
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect();
    for tool in [
        "list_processes",
        "add_process",
        "remove_process",
        "query_logs",
        "set_log_level",
        "set_buffer_size",
    ] {
        assert!(tools.contains(&tool.to_string()), "{tool} in {tools:?}");
    }

    // Added, and refused the second time.
    let dir = program.display().to_string();
    let (ok, added) = call(
        &client,
        "add_process",
        json!({ "dir": dir, "level": "debug" }),
    )
    .await;
    assert!(ok, "{added}");
    let (ok, again) = call(&client, "add_process", json!({ "dir": dir })).await;
    assert!(!ok && again.contains("already watched"), "{again}");

    // Listed with the name and version the program reports, once it has attached.
    let mut listed = Value::Null;
    for _ in 0..50 {
        let (_, text) = call(&client, "list_processes", json!({})).await;
        listed = serde_json::from_str(&text).unwrap();
        if listed[0]["name"] == "watched-test" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(listed[0]["name"], "watched-test");
    assert_eq!(listed[0]["version"], "4.5.6");
    assert_eq!(listed[0]["level"], "debug");

    // What it logs is kept, and read back by line and by time.
    for n in 1..=30 {
        tracing::debug!(target: "test::numbers", n, "number {n}");
    }
    tracing::info!(target: "test::words", "a word");
    let mut log = String::new();
    for _ in 0..50 {
        log = call(
            &client,
            "query_logs",
            json!({ "process": "watched-test", "limit": 100 }),
        )
        .await
        .1;
        if log.contains("a word") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(log.contains("number 1  n=1"), "{log}");
    assert!(log.contains("kept: lines 1 to 31"), "{log}");
    let (_, lines) = call(
        &client,
        "query_logs",
        json!({ "process": "watched-test", "from_line": 10, "to_line": 12 }),
    )
    .await;
    assert!(
        lines.contains("#10 ") && lines.contains("#12 ") && !lines.contains("#13 "),
        "{lines}"
    );
    let (_, recent) = call(
        &client,
        "query_logs",
        json!({ "process": "watched-test", "since": "1m", "level": "info" }),
    )
    .await;
    assert!(
        recent.contains("a word") && !recent.contains("number"),
        "{recent}"
    );
    let (ok, bad) = call(
        &client,
        "query_logs",
        json!({ "process": "watched-test", "since": "yesterday-ish" }),
    )
    .await;
    assert!(!ok && bad.contains("not a time"), "{bad}");

    // The level is changed in the program: debug no longer comes.
    let (ok, set) = call(
        &client,
        "set_log_level",
        json!({ "process": "watched-test", "level": "info" }),
    )
    .await;
    assert!(ok, "{set}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    tracing::debug!(target: "test::numbers", "not wanted now");
    tracing::info!(target: "test::words", "still wanted");
    let mut after = String::new();
    for _ in 0..50 {
        after = call(
            &client,
            "query_logs",
            json!({ "process": "watched-test", "from_line": 32 }),
        )
        .await
        .1;
        if after.contains("still wanted") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        after.contains("still wanted") && !after.contains("not wanted now"),
        "{after}"
    );

    // The buffer is resized within bounds.
    let (ok, resized) = call(
        &client,
        "set_buffer_size",
        json!({ "process": "watched-test", "lines": 500 }),
    )
    .await;
    assert!(ok, "{resized}");
    let (ok, too_big) = call(
        &client,
        "set_buffer_size",
        json!({ "process": "watched-test", "lines": 50_000_000 }),
    )
    .await;
    assert!(!ok && too_big.contains("1000000"), "{too_big}");

    // What is watched is saved, for a restart.
    let saved: Value =
        serde_json::from_str(&std::fs::read_to_string(state.join("processes.json")).unwrap())
            .unwrap();
    assert_eq!(saved[0]["lines"], 500);
    assert_eq!(saved[0]["level"], 9);

    let (ok, removed) = call(
        &client,
        "remove_process",
        json!({ "process": "watched-test" }),
    )
    .await;
    assert!(ok, "{removed}");
    let (_, empty) = call(&client, "list_processes", json!({})).await;
    assert_eq!(empty.trim(), "[]");

    let _ = client.cancel().await;
    stop.cancel();
    let _ = server.await;
    drop(_logging);
    drop(watch);
    let _ = std::fs::remove_dir_all(&program);
    let _ = std::fs::remove_dir_all(&state);
}
