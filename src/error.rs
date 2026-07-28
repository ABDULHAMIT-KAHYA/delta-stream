use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum DeltaError {
    InvalidState(&'static str),
    InvalidPacket(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Compression(String),
    Transport(String),
    ResourceLimit(&'static str),
    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    ProtocolVersion {
        received: u8,
        supported: u8,
    },
    ProtocolVersionRange {
        received: u8,
        min_supported: u8,
        max_supported: u8,
    },
    SchemaMismatch {
        expected: u64,
        received: u64,
    },
    SchemaMigrationMissing {
        from: u64,
        to: u64,
    },
}

impl Display for DeltaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidState(msg) => write!(f, "invalid state: {msg}"),
            Self::InvalidPacket(msg) => write!(f, "invalid packet: {msg}"),
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Json(err) => write!(f, "JSON error: {err}"),
            Self::Compression(msg) => write!(f, "compression error: {msg}"),
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::ResourceLimit(msg) => write!(f, "resource limit: {msg}"),
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
                )
            }
            Self::ProtocolVersion {
                received,
                supported,
            } => {
                write!(
                    f,
                    "unsupported protocol version {received}; supported version is {supported}"
                )
            }
            Self::ProtocolVersionRange {
                received,
                min_supported,
                max_supported,
            } => {
                write!(f, "unsupported protocol version {received}; supported range is {min_supported}..={max_supported}")
            }
            Self::SchemaMismatch { expected, received } => {
                write!(
                    f,
                    "schema mismatch: expected {expected:#x}, got {received:#x}"
                )
            }
            Self::SchemaMigrationMissing { from, to } => {
                write!(
                    f,
                    "no schema migration registered from {from:#x} to {to:#x}"
                )
            }
        }
    }
}

impl std::error::Error for DeltaError {}
impl From<std::io::Error> for DeltaError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<serde_json::Error> for DeltaError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
