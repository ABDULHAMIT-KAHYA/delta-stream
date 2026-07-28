use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use super::CrdtError;

/// Stable replica identifier used as a deterministic CRDT map key and tie-breaker.
///
/// Ordering is lexicographic over the identifier string. [`crate::crdt::LwwRegister`]
/// uses this ordering to break equal-timestamp ties deterministically.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ReplicaId(String);

impl ReplicaId {
    /// Creates a non-empty replica identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, CrdtError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CrdtError::EmptyReplicaId);
        }
        Ok(Self(value))
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ReplicaId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
