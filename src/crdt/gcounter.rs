use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{Crdt, CrdtError, ReplicaId};

/// Grow-only counter CRDT.
///
/// Each replica owns one monotonically increasing component. Merging keeps the
/// component-wise maximum: `merged[replica] = max(local[replica], remote[replica])`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GCounter {
    components: BTreeMap<ReplicaId, u64>,
}

impl GCounter {
    /// Creates an empty counter with value zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments one replica component by `amount`.
    pub fn increment(&mut self, replica: &ReplicaId, amount: u64) -> Result<(), CrdtError> {
        let entry = self.components.entry(replica.clone()).or_insert(0);
        *entry = entry
            .checked_add(amount)
            .ok_or(CrdtError::CounterOverflow)?;
        Ok(())
    }

    /// Returns the sum of all replica components.
    pub fn value(&self) -> Result<u64, CrdtError> {
        self.components.values().try_fold(0_u64, |total, value| {
            total.checked_add(*value).ok_or(CrdtError::ValueOverflow)
        })
    }

    /// Returns one replica component, or zero when the replica has not contributed.
    pub fn component(&self, replica: &ReplicaId) -> u64 {
        self.components.get(replica).copied().unwrap_or(0)
    }

    /// Iterates over components in deterministic replica-id order.
    pub fn components(&self) -> impl Iterator<Item = (&ReplicaId, &u64)> {
        self.components.iter()
    }
}

impl Crdt for GCounter {
    fn merge(&mut self, other: &Self) -> bool {
        let mut changed = false;
        for (replica, remote) in &other.components {
            let local = self.components.entry(replica.clone()).or_insert(0);
            if *remote > *local {
                *local = *remote;
                changed = true;
            }
        }
        changed
    }
}
