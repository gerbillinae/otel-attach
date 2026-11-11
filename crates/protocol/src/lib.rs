//! The attach protocol (openspec `protocol`): the messages a watched program and an attacher
//! exchange over ZeroMQ, the filter an attacher asks for, OpenTelemetry severities, and the
//! discovery file that says where a watched program listens. Pure: no sockets, no runtime.
//!
//! Every message is a ZeroMQ multipart message: a type frame (ASCII) and a body frame, JSON for
//! control messages and OTLP protobuf (`ExportLogsServiceRequest`) for records.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The protocol version this crate speaks. Fields are only ever added; unknown message types and
/// fields are ignored, so a newer side works with an older one for what both know.
pub const PROTOCOL: u32 = 1;

/// The file in a watched program's directory saying where it listens.
pub const DISCOVERY_FILE: &str = "attach.json";

/// Where a watched program listens, and the token an attacher must give.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Discovery {
    pub protocol: u32,
    /// `tcp://127.0.0.1:<port>`.
    pub endpoint: String,
    /// Random, new at every start, readable by the user alone.
    pub token: String,
    pub pid: u32,
}

impl Discovery {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join(DISCOVERY_FILE)
    }

    pub fn read(dir: &Path) -> io::Result<Self> {
        let text = fs::read_to_string(Self::path(dir))?;
        serde_json::from_str(&text)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Written whole (a temporary file renamed over it), so a reader never sees half of it, and
    /// readable by the user alone where the system has modes.
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        fs::create_dir_all(dir)?;
        let path = Self::path(dir);
        let fresh = path.with_extension(format!("json.{}", self.pid));
        let text = serde_json::to_vec_pretty(self).expect("serializable");
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        {
            use std::io::Write;
            let mut file = options.open(&fresh)?;
            file.write_all(&text)?;
        }
        fs::rename(&fresh, &path)
    }

    /// Removes the file if it still names `pid` (a newer copy may have written its own).
    pub fn remove_if_ours(dir: &Path, pid: u32) {
        if Self::read(dir).is_ok_and(|found| found.pid == pid) {
            let _ = fs::remove_file(Self::path(dir));
        }
    }
}

/// What an attacher wants: records at `min_severity` or more severe, and, if `targets` is not
/// empty, only from scopes (Rust module paths, as `pointer_share::machine`) starting with one of
/// them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    pub min_severity: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<String>,
}

impl Filter {
    pub fn wants(&self, severity: u8, target: &str) -> bool {
        severity >= self.min_severity.max(1)
            && (self.targets.is_empty()
                || self
                    .targets
                    .iter()
                    .any(|prefix| target.starts_with(prefix.as_str())))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attach {
    pub protocol: u32,
    pub token: String,
    #[serde(flatten)]
    pub filter: Filter,
    /// Who attaches, for the watched program's own log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Who the watched program is (OpenTelemetry resource attributes: `service.name`,
/// `service.version`, `service.instance.id`, `process.pid`, `host.name`, `os.type`,
/// `process.start_time`) and how it stands now (its own key/value pairs).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct About {
    pub resource: BTreeMap<String, String>,
    #[serde(default)]
    pub status: BTreeMap<String, String>,
}

/// One message, decoded.
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    // Attacher to watched.
    Attach(Attach),
    Level(Filter),
    Ping,
    Detach,
    // Watched to attacher.
    Welcome {
        protocol: u32,
    },
    Refused {
        reason: String,
        protocol: u32,
    },
    About(About),
    /// An OTLP `ExportLogsServiceRequest`, protobuf.
    Logs(Vec<u8>),
    Dropped {
        count: u64,
    },
    Pong,
    Bye {
        reason: String,
    },
    /// A type this version does not know: ignored.
    Unknown(String),
}

#[derive(Serialize, Deserialize)]
struct WelcomeBody {
    protocol: u32,
}

#[derive(Serialize, Deserialize)]
struct RefusedBody {
    reason: String,
    protocol: u32,
}

#[derive(Serialize, Deserialize)]
struct DroppedBody {
    count: u64,
}

#[derive(Serialize, Deserialize)]
struct ByeBody {
    reason: String,
}

impl Message {
    pub fn kind(&self) -> &str {
        match self {
            Message::Attach(_) => "attach",
            Message::Level(_) => "level",
            Message::Ping => "ping",
            Message::Detach => "detach",
            Message::Welcome { .. } => "welcome",
            Message::Refused { .. } => "refused",
            Message::About(_) => "about",
            Message::Logs(_) => "logs",
            Message::Dropped { .. } => "dropped",
            Message::Pong => "pong",
            Message::Bye { .. } => "bye",
            Message::Unknown(kind) => kind,
        }
    }

    /// Its two frames: the type and the body.
    pub fn encode(&self) -> [Vec<u8>; 2] {
        let json = |value: &dyn erased::Json| value.to_json();
        let body = match self {
            Message::Attach(attach) => json(attach),
            Message::Level(filter) => json(filter),
            Message::Ping | Message::Detach | Message::Pong | Message::Unknown(_) => b"{}".to_vec(),
            Message::Welcome { protocol } => json(&WelcomeBody {
                protocol: *protocol,
            }),
            Message::Refused { reason, protocol } => json(&RefusedBody {
                reason: reason.clone(),
                protocol: *protocol,
            }),
            Message::About(about) => json(about),
            Message::Logs(bytes) => bytes.clone(),
            Message::Dropped { count } => json(&DroppedBody { count: *count }),
            Message::Bye { reason } => json(&ByeBody {
                reason: reason.clone(),
            }),
        };
        [self.kind().as_bytes().to_vec(), body]
    }

    /// A message from its frames (the type and the body; a ROUTER's identity frame already
    /// taken off).
    pub fn decode(kind: &[u8], body: &[u8]) -> Result<Self, String> {
        fn json<'a, T: Deserialize<'a>>(body: &'a [u8]) -> Result<T, String> {
            let body = if body.is_empty() {
                b"{}".as_slice()
            } else {
                body
            };
            serde_json::from_slice(body).map_err(|error| error.to_string())
        }
        let kind = std::str::from_utf8(kind).map_err(|error| error.to_string())?;
        Ok(match kind {
            "attach" => Message::Attach(json(body)?),
            "level" => Message::Level(json(body)?),
            "ping" => Message::Ping,
            "detach" => Message::Detach,
            "welcome" => Message::Welcome {
                protocol: json::<WelcomeBody>(body)?.protocol,
            },
            "refused" => {
                let refused: RefusedBody = json(body)?;
                Message::Refused {
                    reason: refused.reason,
                    protocol: refused.protocol,
                }
            }
            "about" => Message::About(json(body)?),
            "logs" => Message::Logs(body.to_vec()),
            "dropped" => Message::Dropped {
                count: json::<DroppedBody>(body)?.count,
            },
            "pong" => Message::Pong,
            "bye" => Message::Bye {
                reason: json::<ByeBody>(body)?.reason,
            },
            other => Message::Unknown(other.to_string()),
        })
    }
}

/// Serializing any body behind one function.
mod erased {
    pub trait Json {
        fn to_json(&self) -> Vec<u8>;
    }

    impl<T: serde::Serialize> Json for T {
        fn to_json(&self) -> Vec<u8> {
            serde_json::to_vec(self).expect("serializable")
        }
    }
}

/// OpenTelemetry severity numbers: TRACE 1–4, DEBUG 5–8, INFO 9–12, WARN 13–16, ERROR 17–20,
/// FATAL 21–24.
pub mod severity {
    pub const TRACE: u8 = 1;
    pub const DEBUG: u8 = 5;
    pub const INFO: u8 = 9;
    pub const WARN: u8 = 13;
    pub const ERROR: u8 = 17;
    pub const FATAL: u8 = 21;

    /// A level by name, any case: `trace`, `debug`, `info`, `warn`, `error`, `fatal`.
    pub fn parse(name: &str) -> Option<u8> {
        Some(match name.to_ascii_lowercase().as_str() {
            "trace" => TRACE,
            "debug" => DEBUG,
            "info" => INFO,
            "warn" | "warning" => WARN,
            "error" => ERROR,
            "fatal" => FATAL,
            _ => return None,
        })
    }

    /// The name of the range a number falls in.
    pub fn name(number: u8) -> &'static str {
        match number {
            0 => "?",
            1..=4 => "TRACE",
            5..=8 => "DEBUG",
            9..=12 => "INFO",
            13..=16 => "WARN",
            17..=20 => "ERROR",
            _ => "FATAL",
        }
    }
}

/// A time as RFC 3339 in UTC, to the millisecond: `2026-11-11T15:02:03.123Z`.
pub fn rfc3339(unix_nanos: u64) -> String {
    let seconds = unix_nanos / 1_000_000_000;
    let millis = (unix_nanos / 1_000_000) % 1000;
    let (days, of_day) = (seconds / 86_400, seconds % 86_400);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        of_day / 3600,
        (of_day / 60) % 60,
        of_day % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_goes_there_and_back() {
        let messages = [
            Message::Attach(Attach {
                protocol: PROTOCOL,
                token: "abc".into(),
                filter: Filter {
                    min_severity: severity::DEBUG,
                    targets: vec!["pointer_share".into()],
                },
                name: Some("test".into()),
            }),
            Message::Level(Filter {
                min_severity: severity::WARN,
                targets: Vec::new(),
            }),
            Message::Ping,
            Message::Detach,
            Message::Welcome { protocol: 1 },
            Message::Refused {
                reason: "wrong token".into(),
                protocol: 1,
            },
            Message::About(About {
                resource: BTreeMap::from([("service.name".into(), "x".into())]),
                status: BTreeMap::from([("role".into(), "owner".into())]),
            }),
            Message::Logs(vec![1, 2, 3]),
            Message::Dropped { count: 7 },
            Message::Pong,
            Message::Bye {
                reason: "replaced".into(),
            },
        ];
        for message in messages {
            let [kind, body] = message.encode();
            assert_eq!(Message::decode(&kind, &body), Ok(message));
        }
        assert_eq!(
            Message::decode(b"metrics", b"{}"),
            Ok(Message::Unknown("metrics".into()))
        );
        // Fields this version does not know are ignored.
        assert_eq!(
            Message::decode(b"dropped", br#"{"count": 2, "since": 5}"#),
            Ok(Message::Dropped { count: 2 })
        );
    }

    #[test]
    fn a_filter_wants_its_severity_and_targets() {
        let filter = Filter {
            min_severity: severity::INFO,
            targets: vec!["pointer_share".into()],
        };
        assert!(filter.wants(severity::WARN, "pointer_share::machine"));
        assert!(!filter.wants(severity::DEBUG, "pointer_share::machine"));
        assert!(!filter.wants(severity::ERROR, "peer_link"));
        assert!(Filter::default().wants(severity::TRACE, "anything"));
    }

    #[test]
    fn severities_have_names() {
        assert_eq!(severity::parse("Info"), Some(9));
        assert_eq!(severity::parse("nonsense"), None);
        assert_eq!(severity::name(10), "INFO");
        assert_eq!(severity::name(17), "ERROR");
    }

    #[test]
    fn times_are_written_as_rfc3339() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000Z");
        // 2026-11-11T15:02:03.123Z
        assert_eq!(
            rfc3339(1_794_409_323_123_000_000),
            "2026-11-11T15:02:03.123Z"
        );
        assert_eq!(rfc3339(951_782_400_000_000_000), "2000-02-29T00:00:00.000Z");
    }

    #[test]
    fn the_discovery_file_is_written_whole_and_for_the_user_alone() {
        let dir = std::env::temp_dir().join(format!("otel-attach-{}", std::process::id()));
        let discovery = Discovery {
            protocol: PROTOCOL,
            endpoint: "tcp://127.0.0.1:5000".into(),
            token: "t".into(),
            pid: 42,
        };
        discovery.write(&dir).unwrap();
        assert_eq!(Discovery::read(&dir).unwrap(), discovery);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(Discovery::path(&dir))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        Discovery::remove_if_ours(&dir, 41);
        assert!(
            Discovery::path(&dir).exists(),
            "another copy's file is kept"
        );
        Discovery::remove_if_ours(&dir, 42);
        assert!(!Discovery::path(&dir).exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
