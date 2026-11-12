//! otel-attach: `watch` attaches to one program and writes its records to stdout; `serve` runs
//! the long-lived service whose MCP tools let an agent watch programs, keep their recent records
//! in ring buffers, and read them back (openspec `attacher`, `service`).

pub mod mcp;
pub mod monitors;
pub mod watch;
