use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use otel_attach::monitors::{Monitors, home, write_private};
use otel_attach::{mcp, watch};
use otel_attach_client::Target;
use otel_attach_protocol::{Filter, severity};
use tokio_util::sync::CancellationToken;

/// The service's port unless told otherwise: fixed, so an agent's configuration keeps working.
const DEFAULT_PORT: u16 = 47_400;

#[derive(Parser)]
#[command(
    version,
    about = "Watch running programs' logs: attach to one, or serve them to an agent over MCP"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Attach to one program and write its records to stdout until interrupted.
    Watch {
        /// The program's directory, holding its `attach.json` (also `OTEL_ATTACH_DIR`).
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
        /// Only records from scopes starting with this (a module path); repeatable.
        #[arg(long = "target")]
        targets: Vec<String>,
        /// One OTLP JSON object per record, and `about` as JSON, instead of lines.
        #[arg(long)]
        json: bool,
    },
    /// Run the service: an MCP server (streamable HTTP on the loopback address) whose tools watch
    /// programs, keep their recent records in ring buffers, and read them back.
    Serve {
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
        /// Where the service keeps its token and what it watches (default `~/.otel-attach`).
        #[arg(long)]
        state: Option<PathBuf>,
    },
}

fn parse_level(name: &str) -> Result<u8, String> {
    severity::parse(name).ok_or_else(|| format!("not a level: {name}"))
}

#[tokio::main]
async fn main() -> ExitCode {
    match Args::parse().command {
        Command::Watch {
            dir,
            endpoint,
            token,
            level,
            targets,
            json,
        } => {
            let target = match (dir, endpoint, token) {
                (Some(dir), _, _) => Target::Dir(dir),
                (None, Some(endpoint), Some(token)) => Target::Fixed { endpoint, token },
                _ => {
                    eprintln!(
                        "otel-attach: give --dir (or OTEL_ATTACH_DIR), or --endpoint and --token"
                    );
                    return ExitCode::FAILURE;
                }
            };
            watch::run(
                target,
                Filter {
                    min_severity: level,
                    targets,
                },
                json,
            )
            .await;
            ExitCode::SUCCESS
        }
        Command::Serve { port, state } => serve(port, state).await,
    }
}

async fn serve(port: u16, state: Option<PathBuf>) -> ExitCode {
    let Some(state) = state.or_else(|| home().map(|home| home.join(".otel-attach"))) else {
        eprintln!("otel-attach: no home directory; give --state");
        return ExitCode::FAILURE;
    };
    let token = match token(&state) {
        Ok(token) => token,
        Err(error) => {
            eprintln!(
                "otel-attach: cannot keep a token in {}: {error}",
                state.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!(
                "otel-attach: cannot listen on {address} (is a service already running?): {error}"
            );
            return ExitCode::FAILURE;
        }
    };
    let monitors = Monitors::new(Some(state.join("processes.json")));
    monitors.restore();
    let url = format!("http://{address}/mcp");
    eprintln!("otel-attach: serving MCP at {url}");
    eprintln!(
        "  for Claude Code: claude mcp add --transport http otel-attach {url} --header \"Authorization: Bearer $(cat {})\"",
        state.join("token").display()
    );
    let stop = CancellationToken::new();
    let server = tokio::spawn(mcp::serve(listener, monitors, token, stop.clone()));
    let _ = tokio::signal::ctrl_c().await;
    stop.cancel();
    let _ = server.await;
    ExitCode::SUCCESS
}

/// The service's token: made once, kept in `state/token`, readable by the user alone.
fn token(state: &std::path::Path) -> std::io::Result<String> {
    let path = state.join("token");
    if let Ok(token) = std::fs::read_to_string(&path)
        && !token.trim().is_empty()
    {
        return Ok(token.trim().to_string());
    }
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| std::io::Error::other(error.to_string()))?;
    let token: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    write_private(&path, token.as_bytes())?;
    Ok(token)
}
