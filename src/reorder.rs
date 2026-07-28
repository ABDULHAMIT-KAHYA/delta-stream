use std::collections::BTreeMap;

use crate::{
    error::DeltaError,
    packet::{Packet, PacketKind},
    state::AgentState,
    sync::{ApplyResult, Decoder},
};

#[derive(Debug, Clone, PartialEq)]
pub enum ReorderApplyResult {
    Applied {
        sequence: u64,
        state: AgentState,
        drained: usize,
    },
    Buffered {
        sequence: u64,
    },
    NeedRecovery {
        local_sequence: Option<u64>,
        required_sequence: u64,
    },
    Duplicate {
        sequence: u64,
    },
}

#[derive(Debug)]
pub struct ReorderDecoder {
    decoder: Decoder,
    pending: BTreeMap<u64, Packet>,
    max_gap: u64,
    max_pending: usize,
}

impl ReorderDecoder {
    pub fn new(max_gap: u64, max_pending: usize) -> Self {
        Self {
            decoder: Decoder::default(),
            pending: BTreeMap::new(),
            max_gap: max_gap.max(1),
            max_pending: max_pending.max(1),
        }
    }

    pub fn apply(&mut self, packet: Packet) -> Result<ReorderApplyResult, DeltaError> {
        if packet.kind == PacketKind::Delta {
            if let Some(local) = self.decoder.sequence() {
                if packet.base_sequence > local {
                    let gap = packet.base_sequence - local;
                    if gap <= self.max_gap && self.pending.len() < self.max_pending {
                        let sequence = packet.sequence;
                        self.pending.entry(sequence).or_insert(packet);
                        return Ok(ReorderApplyResult::Buffered { sequence });
                    }
                    return Ok(ReorderApplyResult::NeedRecovery {
                        local_sequence: Some(local),
                        required_sequence: packet.base_sequence,
                    });
                }
            }
        }

        match self.decoder.apply_packet(packet)? {
            ApplyResult::Applied {
                sequence,
                mut state,
            } => {
                let mut drained = 0usize;
                while let Some(local) = self.decoder.sequence() {
                    let next_sequence = local.saturating_add(1);
                    let Some(next) = self.pending.remove(&next_sequence) else {
                        break;
                    };
                    match self.decoder.apply_packet(next)? {
                        ApplyResult::Applied {
                            state: next_state, ..
                        } => {
                            state = next_state;
                            drained += 1;
                        }
                        ApplyResult::Duplicate { .. } => {}
                        ApplyResult::NeedSnapshot {
                            local_sequence,
                            required_sequence,
                        } => {
                            return Ok(ReorderApplyResult::NeedRecovery {
                                local_sequence,
                                required_sequence,
                            });
                        }
                    }
                }
                Ok(ReorderApplyResult::Applied {
                    sequence: self.decoder.sequence().unwrap_or(sequence),
                    state,
                    drained,
                })
            }
            ApplyResult::Duplicate { sequence } => Ok(ReorderApplyResult::Duplicate { sequence }),
            ApplyResult::NeedSnapshot {
                local_sequence,
                required_sequence,
            } => Ok(ReorderApplyResult::NeedRecovery {
                local_sequence,
                required_sequence,
            }),
        }
    }

    pub fn state(&self) -> Option<&AgentState> {
        self.decoder.state()
    }
    pub fn sequence(&self) -> Option<u64> {
        self.decoder.sequence()
    }
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
    pub fn clear_pending(&mut self) {
        self.pending.clear();
    }
    pub fn reset(&mut self) {
        self.pending.clear();
        self.decoder.reset();
    }
}

impl Default for ReorderDecoder {
    fn default() -> Self {
        Self::new(8, 64)
    }
}
