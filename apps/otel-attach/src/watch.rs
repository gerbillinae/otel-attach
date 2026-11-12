//! `otel-attach watch`: attach to one program and write its records to stdout, one line each, or
//! one OTLP JSON object each with `--json`, until interrupted.

use std::collections::BTreeMap;

use otel_attach_client::{Event, Record, Target};
use otel_attach_protocol::{About, Filter, rfc3339, severity};
use tokio::sync::mpsc;

pub async fn run(target: Target, filter: Filter, json: bool) {
    let (events, mut received) = mpsc::unbounded_channel();
    let control = otel_attach_client::spawn(
        target.clone(),
        filter,
        format!("otel-attach watch (pid {})", std::process::id()),
        events,
    );
    let mut out = Output {
        json,
        introduced: None,
    };
    loop {
        tokio::select! {
            event = received.recv() => match event {
                Some(Event::Waiting) => out.note(&format!("waiting for the program ({})", target.describe())),
                Some(Event::Attached { endpoint }) => out.note(&format!("attached to {endpoint}")),
                Some(Event::About(about)) => out.about(&about),
                Some(Event::Records(records)) => records.iter().for_each(|record| out.record(record)),
                Some(Event::Dropped(count)) => out.note(&format!("{count} records dropped")),
                Some(Event::Refused(reason)) => out.note(&format!("refused: {reason}")),
                Some(Event::Again(why)) => out.note(&why),
                Some(Event::Note(text)) => out.note(&text),
                None => break,
            },
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    control.stop().await;
}

struct Output {
    json: bool,
    /// The program last introduced, so that only its status is printed again.
    introduced: Option<BTreeMap<String, String>>,
}

impl Output {
    fn note(&self, text: &str) {
        if self.json {
            println!("{}", serde_json::json!({ "type": "note", "text": text }));
        } else {
            println!("# {text}");
        }
    }

    fn about(&mut self, about: &About) {
        if self.json {
            println!(
                "{}",
                serde_json::json!({ "type": "about", "resource": about.resource, "status": about.status })
            );
            return;
        }
        if self.introduced.as_ref() != Some(&about.resource) {
            let get = |key: &str| about.resource.get(key).map(String::as_str).unwrap_or("?");
            println!(
                "# {} {} on {} (pid {}, instance {})",
                get("service.name"),
                get("service.version"),
                get("host.name"),
                get("process.pid"),
                get("service.instance.id")
            );
            self.introduced = Some(about.resource.clone());
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

    fn record(&self, record: &Record) {
        if self.json {
            let mut value = serde_json::to_value(&record.otlp).unwrap_or_default();
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "scope".into(),
                    serde_json::Value::String(record.scope.clone()),
                );
            }
            println!("{value}");
        } else {
            println!("{}", line(record));
        }
    }
}

/// `2026-11-11T15:02:03.123Z INFO  pointer_share::service  role changed  role=owner`
pub fn line(record: &Record) -> String {
    let mut line = format!(
        "{} {:<5} {}  {}",
        rfc3339(record.time),
        severity::name(record.severity),
        record.scope,
        record.body
    );
    for (key, value) in &record.attributes {
        line.push_str(&format!("  {key}={value}"));
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_is_one_line() {
        let record = Record {
            time: 1_794_409_323_123_000_000,
            severity: 9,
            scope: "pointer_share::service".into(),
            body: "role changed".into(),
            attributes: vec![("role".into(), "owner".into())],
            otlp: Default::default(),
        };
        assert_eq!(
            line(&record),
            "2026-11-11T15:02:03.123Z INFO  pointer_share::service  role changed  role=owner"
        );
    }
}
