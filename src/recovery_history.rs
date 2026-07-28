use std::collections::VecDeque;

use crate::{error::DeltaError, packet::Packet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryPlan {
    Replay(Vec<Packet>),
    Snapshot(Packet),
}

#[derive(Debug, Clone)]
pub struct RecoveryHistory {
    packets: VecDeque<Packet>,
    max_packets: usize,
    max_bytes: usize,
    bytes: usize,
}

impl RecoveryHistory {
    pub fn new(max_packets: usize, max_bytes: usize) -> Self {
        Self {
            packets: VecDeque::with_capacity(max_packets.max(1)),
            max_packets: max_packets.max(1),
            max_bytes: max_bytes.max(1),
            bytes: 0,
        }
    }

    pub fn record(&mut self, packet: Packet) {
        self.bytes = self.bytes.saturating_add(packet.encoded_len());
        self.packets.push_back(packet);
        while self.packets.len() > self.max_packets || self.bytes > self.max_bytes {
            if let Some(old) = self.packets.pop_front() {
                self.bytes = self.bytes.saturating_sub(old.encoded_len());
            } else {
                break;
            }
        }
    }

    pub fn plan(
        &self,
        local_sequence: Option<u64>,
        snapshot: Packet,
    ) -> Result<RecoveryPlan, DeltaError> {
        let Some(local) = local_sequence else {
            return Ok(RecoveryPlan::Snapshot(snapshot));
        };
        if local >= snapshot.sequence {
            return Ok(RecoveryPlan::Snapshot(snapshot));
        }
        let required_first = local
            .checked_add(1)
            .ok_or(DeltaError::InvalidState("sequence exhausted"))?;
        let replay: Vec<Packet> = self
            .packets
            .iter()
            .filter(|p| p.sequence >= required_first && p.sequence <= snapshot.sequence)
            .cloned()
            .collect();
        if replay.is_empty() {
            return Ok(RecoveryPlan::Snapshot(snapshot));
        }

        let mut expected = required_first;
        let mut bytes = 0usize;
        for packet in &replay {
            if packet.sequence != expected || packet.base_sequence != expected.saturating_sub(1) {
                return Ok(RecoveryPlan::Snapshot(snapshot));
            }
            bytes = bytes.saturating_add(packet.encoded_len());
            expected = expected.saturating_add(1);
        }
        if expected.saturating_sub(1) != snapshot.sequence || bytes >= snapshot.encoded_len() {
            return Ok(RecoveryPlan::Snapshot(snapshot));
        }
        Ok(RecoveryPlan::Replay(replay))
    }

    pub fn len(&self) -> usize {
        self.packets.len()
    }
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Default for RecoveryHistory {
    fn default() -> Self {
        Self::new(256, 8 * 1024 * 1024)
    }
}
