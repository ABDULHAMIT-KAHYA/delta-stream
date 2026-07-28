use serde::{Deserialize, Serialize};

use super::{Crdt, ReplicaId};

/// Deterministic last-write-wins register.
///
/// Applications supply timestamps. DeltaStream does not interpret them as wall-clock
/// time and does not make clock-safety claims. Prefer monotonic logical timestamps,
/// Lamport clocks, or another ordering source suitable for the application.
///
/// Writes are ordered by `(timestamp, replica_id)`, so equal timestamps are resolved
/// deterministically by [`ReplicaId`] ordering. The value itself is not part of the
/// ordering and does not need to implement `Ord`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LwwRegister<T> {
    value: T,
    timestamp: u64,
    replica: ReplicaId,
}

impl<T> LwwRegister<T> {
    /// Creates a register with application-supplied metadata.
    pub fn new(value: T, timestamp: u64, replica: ReplicaId) -> Self {
        Self {
            value,
            timestamp,
            replica,
        }
    }

    /// Returns the current value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Returns the application-supplied timestamp for the winning value.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the replica that produced the winning value.
    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }

    /// Assigns a value if its `(timestamp, replica_id)` metadata wins.
    pub fn assign(&mut self, value: T, timestamp: u64, replica: ReplicaId) -> bool {
        if (timestamp, &replica) > (self.timestamp, &self.replica) {
            self.value = value;
            self.timestamp = timestamp;
            self.replica = replica;
            return true;
        }
        false
    }
}

impl<T: Clone> Crdt for LwwRegister<T> {
    fn merge(&mut self, other: &Self) -> bool {
        self.assign(other.value.clone(), other.timestamp, other.replica.clone())
    }
}
