//! A program to attach to: it logs at every level and counts in its status, until stopped.
//! `cargo run --example demo -- <dir>`, then `otel-attach --dir <dir> --level debug`.

use std::time::Duration;

use tracing_subscriber::prelude::*;

fn main() {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/otel-attach-demo".into());
    let watch = otel_attach_watched::Watch::start(otel_attach_watched::Config {
        dir: dir.into(),
        name: "demo".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        instance: None,
    })
    .expect("listening");
    tracing_subscriber::registry().with(watch.layer()).init();
    eprintln!("demo: attach at {}", watch.endpoint());
    for tick in 0u64.. {
        tracing::trace!(tick, "a trace record");
        tracing::debug!(tick, "a debug record");
        if tick % 5 == 0 {
            tracing::info!(target: "demo::counter", tick, "counted to {tick}");
            watch.set_status("tick", tick.to_string());
        }
        if tick % 20 == 0 {
            tracing::warn!(tick, even = tick % 2 == 0, "a warning");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
