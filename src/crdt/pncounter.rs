use serde::{Deserialize, Serialize};

use super::{Crdt, CrdtError, GCounter, ReplicaId};

/// Positive-negative counter CRDT built from two grow-only counters.
///
/// The public value is `sum(positive components) - sum(negative components)`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PNCounter {
    positive: GCounter,
    negative: GCounter,
}

impl PNCounter {
    /// Creates a counter with value zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `amount` to this replica's positive component.
    pub fn increment(&mut self, replica: &ReplicaId, amount: u64) -> Result<(), CrdtError> {
        self.positive.increment(replica, amount)
    }

    /// Adds `amount` to this replica's negative component.
    pub fn decrement(&mut self, replica: &ReplicaId, amount: u64) -> Result<(), CrdtError> {
        self.negative.increment(replica, amount)
    }

    /// Returns the signed counter value.
    pub fn value(&self) -> Result<i128, CrdtError> {
        let positive = i128::from(self.positive.value()?);
        let negative = i128::from(self.negative.value()?);
        positive
            .checked_sub(negative)
            .ok_or(CrdtError::ValueOverflow)
    }

    /// Returns the read-only positive grow-only counter.
    pub fn positive(&self) -> &GCounter {
        &self.positive
    }

    /// Returns the read-only negative grow-only counter.
    pub fn negative(&self) -> &GCounter {
        &self.negative
    }
}

impl Crdt for PNCounter {
    fn merge(&mut self, other: &Self) -> bool {
        let positive_changed = self.positive.merge(&other.positive);
        let negative_changed = self.negative.merge(&other.negative);
        positive_changed || negative_changed
    }
}
