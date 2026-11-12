//! The service's MCP server (openspec `service`): tools for an agent to watch processes, read
//! their logs back by time or by line, and change what is kept. Streamable HTTP on the loopback
//! address, behind a bearer token.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use otel_attach_client::Target;
use otel_attach_protocol::{Filter, rfc3339, severity};
use otel_attach_ring::{FEWEST_LINES, MOST_LINES, Query};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData, ServerHandler, schemars, tool, tool_handler, tool_router};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::monitors::{DEFAULT_LINES, Monitors, expand, now};

/// A query returns this many lines unless asked for fewer, and never more than the most.
const DEFAULT_LIMIT: usize = 200;
const MOST_LIMIT: usize = 2000;

const INSTRUCTIONS: &str = "otel-attach watches running programs that speak the attach protocol \
(they write attach.json in a directory of their own) and keeps each one's recent log records in a \
ring buffer. add_process starts watching one (by its directory, e.g. ~/.unified-inputs); \
list_processes shows what is watched, with each program's name and version as it reports them, \
and how much of its log is kept; query_logs reads the log back, by time (since/until: RFC 3339, \
or a duration ago such as 10m) or by line (from_line/to_line: line numbers count up from 1 and \
are never reused); set_log_level changes what a process sends; set_buffer_size changes how many \
lines are kept. A higher level (debug, trace) explains more, costs the program more, and fills \
the buffer faster.";

#[derive(Clone)]
pub struct Tools {
    monitors: Arc<Monitors>,
    #[expect(dead_code, reason = "read by the code tool_handler generates")]
    tool_router: ToolRouter<Self>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct AddProcess {
    /// The program's directory, holding its attach.json (for unified-inputs: ~/.unified-inputs).
    /// Followed across restarts of the program.
    pub dir: Option<String>,
    /// Instead of `dir`: where the program listens (tcp://127.0.0.1:PORT), with `token`.
    pub endpoint: Option<String>,
    pub token: Option<String>,
    /// The least severe records to keep: trace, debug, info (the default), warn, error or fatal.
    pub level: Option<String>,
    /// Only records whose scope (a module path, as pointer_share::machine) starts with one of
    /// these.
    pub targets: Option<Vec<String>>,
    /// How many lines the buffer keeps: 100 to 1000000, 10000 by default.
    pub buffer_lines: Option<usize>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct ProcessOnly {
    /// The process: its id from list_processes, or the name it reports if only one has it.
    pub process: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct QueryLogs {
    /// The process: its id from list_processes, or the name it reports if only one has it.
    pub process: String,
    /// From this time: RFC 3339 (2026-11-12T10:00:00Z), or a duration ago (90s, 10m, 1h30m).
    pub since: Option<String>,
    /// Up to this time, the same way.
    pub until: Option<String>,
    /// From this line (inclusive).
    pub from_line: Option<u64>,
    /// Up to this line (inclusive).
    pub to_line: Option<u64>,
    /// Only records at least this severe: trace, debug, info, warn, error, fatal.
    pub level: Option<String>,
    /// Only records whose scope starts with one of these.
    pub targets: Option<Vec<String>>,
    /// Only records whose text (body or attributes) contains this, any case.
    pub contains: Option<String>,
    /// The most lines to return: 200 by default, 2000 at most. Reading from a line or a time,
    /// the first that match; otherwise the newest.
    pub limit: Option<usize>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct SetLogLevel {
    /// The process: its id from list_processes, or the name it reports if only one has it.
    pub process: String,
    /// trace, debug, info, warn, error or fatal.
    pub level: String,
    /// Only records whose scope starts with one of these; leave out for every scope.
    pub targets: Option<Vec<String>>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct SetBufferSize {
    /// The process: its id from list_processes, or the name it reports if only one has it.
    pub process: String,
    /// Lines to keep: 100 to 1000000. Shrinking lets the oldest go.
    pub lines: usize,
}

fn refusal(text: impl Into<String>) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::error(vec![ContentBlock::text(text)]))
}

fn reply(text: impl Into<String>) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

fn level(name: Option<&str>, default: u8) -> Result<u8, String> {
    match name {
        None => Ok(default),
        Some(name) => severity::parse(name).ok_or_else(|| {
            format!("{name} is not a level: trace, debug, info, warn, error or fatal")
        }),
    }
}

/// A time as unix nanoseconds: RFC 3339, or a duration ago.
fn when(text: &str) -> Result<u64, String> {
    if let Ok(ago) = humantime::parse_duration(text) {
        return Ok(now().saturating_sub(ago.as_nanos() as u64));
    }
    humantime::parse_rfc3339_weak(text)
        .map(|time| {
            time.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        })
        .map_err(|_| {
            format!(
                "{text} is not a time: give RFC 3339 (2026-11-12T10:00:00Z) or a duration ago (10m)"
            )
        })
}

#[tool_router]
impl Tools {
    pub fn new(monitors: Arc<Monitors>) -> Self {
        Self {
            monitors,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "The processes being watched: each one's id (what the other tools take), its name, version, pid, host and status as the process reports them, whether it is attached, the level asked for, and its buffer (lines kept, first and last line numbers, oldest and newest times).",
        annotations(read_only_hint = true)
    )]
    async fn list_processes(&self) -> Result<CallToolResult, ErrorData> {
        reply(serde_json::to_string_pretty(&self.monitors.list()).expect("serializable"))
    }

    #[tool(
        description = "Start watching a process: by its directory (holding attach.json; followed across restarts of the program), or by endpoint and token. Refused if it is already watched. Its records at the level asked for (info by default) are kept in a buffer of its own from now on."
    )]
    async fn add_process(
        &self,
        Parameters(add): Parameters<AddProcess>,
    ) -> Result<CallToolResult, ErrorData> {
        let target = match (add.dir, add.endpoint, add.token) {
            (Some(dir), None, _) => Target::Dir(expand(&dir).into()),
            (None, Some(endpoint), Some(token)) => Target::Fixed { endpoint, token },
            _ => return refusal("give dir, or endpoint and token"),
        };
        let min_severity = match level(add.level.as_deref(), severity::INFO) {
            Ok(level) => level,
            Err(error) => return refusal(error),
        };
        let filter = Filter {
            min_severity,
            targets: add.targets.unwrap_or_default(),
        };
        match self
            .monitors
            .add(target, filter, add.buffer_lines.unwrap_or(DEFAULT_LINES))
        {
            Ok(id) => {
                // A moment to attach, so the reply can say how it went.
                tokio::time::sleep(Duration::from_millis(300)).await;
                let listed = self
                    .monitors
                    .list()
                    .into_iter()
                    .find(|listed| listed.id == id);
                reply(format!(
                    "watching {id}: {}",
                    listed.map_or_else(String::new, |listed| match (listed.name, listed.version) {
                        (Some(name), Some(version)) =>
                            format!("{name} {version}, {}", listed.state),
                        _ => listed.state,
                    })
                ))
            }
            Err(error) => refusal(error),
        }
    }

    #[tool(description = "Stop watching a process, and let its buffer go.")]
    async fn remove_process(
        &self,
        Parameters(remove): Parameters<ProcessOnly>,
    ) -> Result<CallToolResult, ErrorData> {
        match self.monitors.remove(&remove.process) {
            Ok(id) => reply(format!("no longer watching {id}")),
            Err(error) => refusal(error),
        }
    }

    #[tool(
        description = "Read a process's kept log back, by time range (since/until) or line range (from_line/to_line), or both, narrowed by level, scope and text. One line per record: #line time severity scope body key=value. With neither a time nor a line to start from, the newest records come.",
        annotations(read_only_hint = true)
    )]
    async fn query_logs(
        &self,
        Parameters(asked): Parameters<QueryLogs>,
    ) -> Result<CallToolResult, ErrorData> {
        let since = match asked.since.as_deref().map(when).transpose() {
            Ok(since) => since,
            Err(error) => return refusal(error),
        };
        let until = match asked.until.as_deref().map(when).transpose() {
            Ok(until) => until,
            Err(error) => return refusal(error),
        };
        let min_severity = match level(asked.level.as_deref(), 0) {
            Ok(level) => level,
            Err(error) => return refusal(error),
        };
        let limit = asked.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MOST_LIMIT);
        let query = Query {
            from_line: asked.from_line,
            to_line: asked.to_line,
            since,
            until,
            min_severity,
            targets: asked.targets.unwrap_or_default(),
            contains: asked.contains,
            limit,
        };
        let (id, about, found) = match self.monitors.query(&asked.process, &query) {
            Ok(found) => found,
            Err(error) => return refusal(error),
        };
        let mut text = String::new();
        let name = about
            .as_ref()
            .and_then(|about| {
                Some(format!(
                    "{} {}",
                    about.resource.get("service.name")?,
                    about.resource.get("service.version")?
                ))
            })
            .unwrap_or_else(|| "not yet introduced".into());
        let _ = writeln!(text, "{id} ({name})");
        let _ = match (found.first_kept, found.last_kept) {
            (Some(first), Some(last)) => writeln!(
                text,
                "kept: lines {first} to {last} ({} let go before)",
                found.let_go
            ),
            _ => writeln!(text, "kept: nothing yet"),
        };
        let _ = write!(text, "returned: {} line(s)", found.entries.len());
        if found.truncated {
            let forward = query.from_line.is_some() || query.since.is_some();
            match (forward, found.entries.last(), found.entries.first()) {
                (true, Some(last), _) => {
                    let _ = write!(
                        text,
                        ", more match: continue with from_line {}",
                        last.line + 1
                    );
                }
                (false, _, Some(first)) => {
                    let _ = write!(
                        text,
                        ", more match before: continue with to_line {}",
                        first.line - 1
                    );
                }
                _ => {}
            }
        }
        text.push('\n');
        for entry in &found.entries {
            let _ = write!(
                text,
                "#{} {} {:<5} {}  {}",
                entry.line,
                rfc3339(entry.time),
                severity::name(entry.severity),
                entry.scope,
                entry.body
            );
            for (key, value) in &entry.attributes {
                let _ = write!(text, "  {key}={value}");
            }
            text.push('\n');
        }
        reply(text)
    }

    #[tool(
        description = "Change the level (and optionally the scopes) a process sends, from now on: trace, debug, info, warn, error or fatal. Records already kept stay."
    )]
    async fn set_log_level(
        &self,
        Parameters(set): Parameters<SetLogLevel>,
    ) -> Result<CallToolResult, ErrorData> {
        let min_severity = match level(Some(&set.level), severity::INFO) {
            Ok(level) => level,
            Err(error) => return refusal(error),
        };
        let filter = Filter {
            min_severity,
            targets: set.targets.unwrap_or_default(),
        };
        match self.monitors.set_filter(&set.process, filter) {
            Ok(id) => reply(format!(
                "{id} now sends {} and above",
                set.level.to_lowercase()
            )),
            Err(error) => refusal(error),
        }
    }

    #[tool(
        description = "Change how many lines a process's buffer keeps: 100 to 1000000. Shrinking lets the oldest lines go at once."
    )]
    async fn set_buffer_size(
        &self,
        Parameters(set): Parameters<SetBufferSize>,
    ) -> Result<CallToolResult, ErrorData> {
        match self.monitors.set_lines(&set.process, set.lines) {
            Ok((id, lines)) => reply(format!("{id} keeps {lines} lines")),
            Err(error) => refusal(format!("{error} (from {FEWEST_LINES} to {MOST_LINES})")),
        }
    }
}

#[tool_handler]
impl ServerHandler for Tools {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "otel-attach",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(INSTRUCTIONS)
    }
}

/// Serves the tools at `/mcp` on `listener` until `stop`, to requests carrying `token`.
pub async fn serve(
    listener: TcpListener,
    monitors: Arc<Monitors>,
    token: String,
    stop: CancellationToken,
) {
    let service: StreamableHttpService<Tools, LocalSessionManager> = StreamableHttpService::new(
        move || Ok(Tools::new(Arc::clone(&monitors))),
        Default::default(),
        StreamableHttpServerConfig::default().with_cancellation_token(stop.child_token()),
    );
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(Arc::new(token), authorized));
    let _ = axum::serve(listener, router)
        .with_graceful_shutdown(async move { stop.cancelled_owned().await })
        .await;
}

/// Only requests with the service's token: the logs are the user's.
async fn authorized(State(token): State<Arc<String>>, request: Request, next: Next) -> Response {
    let given = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if given == Some(token.as_str()) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            "a bearer token is needed: see ~/.otel-attach/token",
        )
            .into_response()
    }
}
