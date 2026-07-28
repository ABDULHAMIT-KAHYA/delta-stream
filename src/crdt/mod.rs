//! Optional state-based CRDTs for multi-writer replicated values.
//!
//! This module is separate from DeltaStream's authoritative [`crate::Publisher`] and
//! [`crate::Subscriber`] APIs. CRDT values define their own merge behavior and can be
//! serialized into transport-independent bytes with [`encode_crdt`] when the `crdt`
//! feature is enabled.

mod gcounter;
mod lww_register;
mod pncounter;
mod replica_id;

use serde::{de::DeserializeOwned, Serialize};
use std::{
    error::Error,
    fmt::{Display, Formatter},
};

pub use gcounter::GCounter;
pub use lww_register::LwwRegister;
pub use pncounter::PNCounter;
pub use replica_id::ReplicaId;

/// State-based merge semantics for a CRDT value.
///
/// Implementations are responsible for satisfying the usual state-based CRDT laws:
///
/// - commutative: merging `a` with `b` produces the same state as merging `b` with `a`;
/// - associative: merge grouping does not change the final state;
/// - idempotent: merging the same state more than once does not change the result.
///
/// The returned boolean indicates whether `self` changed.
pub trait Crdt {
    /// Merges another replica state into this state.
    fn merge(&mut self, other: &Self) -> bool;
}

/// Decode limits for CRDT values encoded with [`encode_crdt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrdtDecodeConfig {
    /// Maximum encoded JSON byte length accepted by [`decode_crdt`].
    pub max_encoded_bytes: usize,
}

impl Default for CrdtDecodeConfig {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 1024 * 1024,
        }
    }
}

/// Errors returned by the optional CRDT module.
#[derive(Debug)]
pub enum CrdtError {
    /// Replica identifiers must be non-empty.
    EmptyReplicaId,
    /// A per-replica counter component would overflow `u64`.
    CounterOverflow,
    /// A derived CRDT value would overflow its public value type.
    ValueOverflow,
    /// Encoded input exceeded the configured decode limit.
    EncodedValueTooLarge { limit: usize, actual: usize },
    /// Serialization failed.
    Serialization(serde_json::Error),
    /// Deserialization failed.
    Deserialization(serde_json::Error),
}

impl Display for CrdtError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyReplicaId => write!(f, "replica id must not be empty"),
            Self::CounterOverflow => write!(f, "counter component overflow"),
            Self::ValueOverflow => write!(f, "CRDT value overflow"),
            Self::EncodedValueTooLarge { limit, actual } => write!(
                f,
                "encoded CRDT value too large: {actual} bytes exceeds limit {limit} bytes"
            ),
            Self::Serialization(err) => write!(f, "CRDT serialization error: {err}"),
            Self::Deserialization(err) => write!(f, "CRDT deserialization error: {err}"),
        }
    }
}

impl Error for CrdtError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Serialization(err) | Self::Deserialization(err) => Some(err),
            _ => None,
        }
    }
}

/// Encodes a CRDT value as deterministic JSON bytes where the underlying type uses
/// deterministic Serde ordering.
///
/// The CRDT module uses `BTreeMap` for map-backed types so their JSON object keys are
/// emitted in deterministic order by `serde_json`.
pub fn encode_crdt<T: Serialize>(value: &T) -> Result<Vec<u8>, CrdtError> {
    serde_json::to_vec(value).map_err(CrdtError::Serialization)
}

/// Decodes a CRDT value from JSON bytes after enforcing a maximum encoded size.
pub fn decode_crdt<T: DeserializeOwned>(
    bytes: &[u8],
    config: CrdtDecodeConfig,
) -> Result<T, CrdtError> {
    if bytes.len() > config.max_encoded_bytes {
        return Err(CrdtError::EncodedValueTooLarge {
            limit: config.max_encoded_bytes,
            actual: bytes.len(),
        });
    }
    serde_json::from_slice(bytes).map_err(CrdtError::Deserialization)
}
