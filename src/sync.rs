use std::marker::PhantomData;

use crate::{
    adaptive::{AdaptivePolicy, EncodeDecision},
    binary::{
        decode_agent_delta, decode_agent_state, encode_agent_delta, encode_agent_state, AgentDelta,
    },
    error::DeltaError,
    packet::{DecodeConfig, Packet, PacketKind},
    replay::ReplayWindow,
    schema::{
        decode_generic_delta, decode_generic_snapshot, encode_generic_delta,
        encode_generic_snapshot, DeltaState, JsonFieldDelta,
    },
    state::AgentState,
};

pub const AGENT_SCHEMA_NAME: &str = "delta-stream/AgentState/v2";

pub fn agent_schema_hash() -> u64 {
    fnv1a64(AGENT_SCHEMA_NAME.as_bytes())
}

pub fn state_hash(state: &AgentState) -> Result<u64, DeltaError> {
    Ok(fnv1a64(&encode_agent_state(state)?))
}

pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn raw_snapshot(sequence: u64, state: &AgentState) -> Result<Packet, DeltaError> {
    Ok(Packet::snapshot(
        sequence,
        state_hash(state)?,
        agent_schema_hash(),
        encode_agent_state(state)?,
    ))
}

#[derive(Debug, Default)]
pub struct Encoder {
    sequence: u64,
    last_state: Option<AgentState>,
    policy: AdaptivePolicy,
    last_decision: Option<EncodeDecision>,
}

impl Encoder {
    pub fn with_policy(policy: AdaptivePolicy) -> Self {
        Self {
            policy,
            ..Self::default()
        }
    }

    /// Builds safe snapshot and delta candidates and lets the configured policy
    /// policy choose raw or zstd-compressed representation.
    pub fn encode(&mut self, state: &AgentState) -> Result<Packet, DeltaError> {
        self.sequence = self.sequence.saturating_add(1);
        let sequence = self.sequence;
        let snapshot = raw_snapshot(sequence, state)?;

        let packet = match &self.last_state {
            None => {
                let (selected, decision) = self.policy.select_initial(snapshot)?;
                self.last_decision = Some(decision);
                selected
            }
            Some(previous) => {
                let delta = AgentDelta::between(previous, state);
                let delta = Packet::delta(
                    sequence,
                    sequence - 1,
                    state_hash(previous)?,
                    agent_schema_hash(),
                    encode_agent_delta(&delta)?,
                );
                let (selected, decision) = self.policy.select(snapshot, delta)?;
                self.last_decision = Some(decision);
                selected
            }
        };

        self.last_state = Some(state.clone());
        Ok(packet)
    }

    pub fn force_snapshot(&mut self, state: &AgentState) -> Result<Packet, DeltaError> {
        self.sequence = self.sequence.saturating_add(1);
        self.last_state = Some(state.clone());
        let snapshot = raw_snapshot(self.sequence, state)?;
        let (packet, decision) = self.policy.select_initial(snapshot)?;
        self.last_decision = Some(decision);
        Ok(packet)
    }

    /// Build an authoritative recovery snapshot at the current stream sequence.
    /// Unlike `force_snapshot`, this does not advance the publisher sequence,
    /// preventing one client's recovery from creating gaps for every other client.
    pub fn recovery_snapshot(&self, state: &AgentState) -> Result<Packet, DeltaError> {
        let snapshot = raw_snapshot(self.sequence, state)?;
        Ok(self.policy.select_initial(snapshot)?.0)
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn last_decision(&self) -> Option<EncodeDecision> {
        self.last_decision
    }
    pub fn policy(&self) -> AdaptivePolicy {
        self.policy
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ApplyResult {
    Applied {
        sequence: u64,
        state: AgentState,
    },
    NeedSnapshot {
        local_sequence: Option<u64>,
        required_sequence: u64,
    },
    Duplicate {
        sequence: u64,
    },
}

#[derive(Debug, Default)]
pub struct Decoder {
    sequence: Option<u64>,
    state: Option<AgentState>,
    replay: ReplayWindow,
}

impl Decoder {
    pub fn apply_packet(&mut self, packet: Packet) -> Result<ApplyResult, DeltaError> {
        if packet.schema_hash != agent_schema_hash() {
            return Err(DeltaError::SchemaMismatch {
                expected: agent_schema_hash(),
                received: packet.schema_hash,
            });
        }
        if self.replay.contains(packet.sequence) {
            return Ok(ApplyResult::Duplicate {
                sequence: packet.sequence,
            });
        }
        if self.sequence.is_some_and(|local| packet.sequence < local) {
            return Ok(ApplyResult::Duplicate {
                sequence: packet.sequence,
            });
        }

        let result = match packet.kind {
            PacketKind::Snapshot => {
                let payload = packet.logical_payload_with_config(&DecodeConfig::default())?;
                let state = decode_agent_state(&payload)?;
                if state_hash(&state)? != packet.base_hash {
                    return Err(DeltaError::InvalidState("snapshot hash mismatch"));
                }
                self.sequence = Some(packet.sequence);
                self.state = Some(state.clone());
                ApplyResult::Applied {
                    sequence: packet.sequence,
                    state,
                }
            }
            PacketKind::Delta => {
                if self.sequence != Some(packet.base_sequence) {
                    return Ok(ApplyResult::NeedSnapshot {
                        local_sequence: self.sequence,
                        required_sequence: packet.base_sequence,
                    });
                }
                let current = self
                    .state
                    .as_ref()
                    .ok_or(DeltaError::InvalidState("decoder has no base state"))?;
                if state_hash(current)? != packet.base_hash {
                    return Ok(ApplyResult::NeedSnapshot {
                        local_sequence: self.sequence,
                        required_sequence: packet.base_sequence,
                    });
                }
                let payload = packet.logical_payload_with_config(&DecodeConfig::default())?;
                let delta = decode_agent_delta(&payload)?;
                let next = delta.apply(current)?;
                self.sequence = Some(packet.sequence);
                self.state = Some(next.clone());
                ApplyResult::Applied {
                    sequence: packet.sequence,
                    state: next,
                }
            }
        };

        self.replay.record(packet.sequence);
        Ok(result)
    }

    pub fn state(&self) -> Option<&AgentState> {
        self.state.as_ref()
    }
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }
    pub fn reset(&mut self) {
        self.sequence = None;
        self.state = None;
        self.replay.clear();
    }
}

pub struct GenericEncoder<T: DeltaState> {
    sequence: u64,
    last_state: Option<T>,
    policy: AdaptivePolicy,
    last_decision: Option<EncodeDecision>,
}

impl<T: DeltaState> Default for GenericEncoder<T> {
    fn default() -> Self {
        Self {
            sequence: 0,
            last_state: None,
            policy: AdaptivePolicy::default(),
            last_decision: None,
        }
    }
}

impl<T: DeltaState> GenericEncoder<T> {
    pub fn with_policy(policy: AdaptivePolicy) -> Self {
        Self {
            policy,
            ..Self::default()
        }
    }

    pub fn encode(&mut self, state: &T) -> Result<Packet, DeltaError> {
        self.sequence = self.sequence.saturating_add(1);
        let sequence = self.sequence;
        let bytes = encode_generic_snapshot(state)?;
        let snapshot = Packet::snapshot(sequence, fnv1a64(&bytes), T::schema_hash(), bytes);

        let packet = match &self.last_state {
            None => {
                let (selected, decision) = self.policy.select_initial(snapshot)?;
                self.last_decision = Some(decision);
                selected
            }
            Some(previous) => {
                let old = encode_generic_snapshot(previous)?;
                let delta = JsonFieldDelta::between(previous, state)?;
                let delta = Packet::delta(
                    sequence,
                    sequence - 1,
                    fnv1a64(&old),
                    T::schema_hash(),
                    encode_generic_delta(&delta)?,
                );
                let (selected, decision) = self.policy.select(snapshot, delta)?;
                self.last_decision = Some(decision);
                selected
            }
        };

        self.last_state = Some(state.clone());
        Ok(packet)
    }

    pub fn force_snapshot(&mut self, state: &T) -> Result<Packet, DeltaError> {
        self.sequence = self.sequence.saturating_add(1);
        let bytes = encode_generic_snapshot(state)?;
        self.last_state = Some(state.clone());
        let snapshot = Packet::snapshot(self.sequence, fnv1a64(&bytes), T::schema_hash(), bytes);
        let (packet, decision) = self.policy.select_initial(snapshot)?;
        self.last_decision = Some(decision);
        Ok(packet)
    }

    pub fn recovery_snapshot(&self, state: &T) -> Result<Packet, DeltaError> {
        let bytes = encode_generic_snapshot(state)?;
        let snapshot = Packet::snapshot(self.sequence, fnv1a64(&bytes), T::schema_hash(), bytes);
        Ok(self.policy.select_initial(snapshot)?.0)
    }

    pub fn last_decision(&self) -> Option<EncodeDecision> {
        self.last_decision
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

pub struct GenericDecoder<T: DeltaState> {
    sequence: Option<u64>,
    state: Option<T>,
    replay: ReplayWindow,
    decode_config: DecodeConfig,
    _marker: PhantomData<T>,
}

impl<T: DeltaState> Default for GenericDecoder<T> {
    fn default() -> Self {
        Self {
            sequence: None,
            state: None,
            replay: ReplayWindow::default(),
            decode_config: DecodeConfig::default(),
            _marker: PhantomData,
        }
    }
}

/// Result of applying a packet to a subscriber.
///
/// `Applied` contains the committed state after a snapshot or delta. `Duplicate` means
/// the packet was already seen or is stale. `NeedSnapshot` means the subscriber could
/// not safely apply a delta because its required base sequence or base hash is missing.
#[derive(Debug, Clone, PartialEq)]
pub enum GenericApplyResult<T> {
    Applied {
        sequence: u64,
        state: T,
    },
    NeedSnapshot {
        local_sequence: Option<u64>,
        required_sequence: u64,
    },
    Duplicate {
        sequence: u64,
    },
}

impl<T: DeltaState> GenericDecoder<T> {
    pub fn with_decode_config(decode_config: DecodeConfig) -> Self {
        Self {
            decode_config,
            ..Self::default()
        }
    }

    pub fn apply_packet(&mut self, packet: Packet) -> Result<GenericApplyResult<T>, DeltaError> {
        if packet.schema_hash != T::schema_hash() {
            return Err(DeltaError::SchemaMismatch {
                expected: T::schema_hash(),
                received: packet.schema_hash,
            });
        }
        if self.replay.contains(packet.sequence) {
            return Ok(GenericApplyResult::Duplicate {
                sequence: packet.sequence,
            });
        }
        if self.sequence.is_some_and(|local| packet.sequence < local) {
            return Ok(GenericApplyResult::Duplicate {
                sequence: packet.sequence,
            });
        }

        let result = match packet.kind {
            PacketKind::Snapshot => {
                let payload = packet.logical_payload_with_config(&self.decode_config)?;
                if fnv1a64(&payload) != packet.base_hash {
                    return Err(DeltaError::InvalidState("snapshot hash mismatch"));
                }
                let state = decode_generic_snapshot::<T>(&payload)?;
                self.sequence = Some(packet.sequence);
                self.state = Some(state.clone());
                GenericApplyResult::Applied {
                    sequence: packet.sequence,
                    state,
                }
            }
            PacketKind::Delta => {
                if self.sequence != Some(packet.base_sequence) {
                    return Ok(GenericApplyResult::NeedSnapshot {
                        local_sequence: self.sequence,
                        required_sequence: packet.base_sequence,
                    });
                }
                let current = self
                    .state
                    .as_ref()
                    .ok_or(DeltaError::InvalidState("decoder has no base state"))?;
                let current_bytes = encode_generic_snapshot(current)?;
                if fnv1a64(&current_bytes) != packet.base_hash {
                    return Ok(GenericApplyResult::NeedSnapshot {
                        local_sequence: self.sequence,
                        required_sequence: packet.base_sequence,
                    });
                }
                let payload = packet.logical_payload_with_config(&self.decode_config)?;
                let delta = decode_generic_delta(&payload)?;
                let next = delta.apply(current)?;
                self.sequence = Some(packet.sequence);
                self.state = Some(next.clone());
                GenericApplyResult::Applied {
                    sequence: packet.sequence,
                    state: next,
                }
            }
        };

        self.replay.record(packet.sequence);
        Ok(result)
    }
    pub fn apply_packet_with_migrations(
        &mut self,
        packet: Packet,
        migrations: &crate::migration::MigrationRegistry,
    ) -> Result<GenericApplyResult<T>, DeltaError> {
        if packet.schema_hash == T::schema_hash() {
            return self.apply_packet(packet);
        }
        if packet.kind != PacketKind::Snapshot {
            return Ok(GenericApplyResult::NeedSnapshot {
                local_sequence: self.sequence,
                required_sequence: packet.base_sequence,
            });
        }
        if self.replay.contains(packet.sequence) {
            return Ok(GenericApplyResult::Duplicate {
                sequence: packet.sequence,
            });
        }
        if self.sequence.is_some_and(|local| packet.sequence < local) {
            return Ok(GenericApplyResult::Duplicate {
                sequence: packet.sequence,
            });
        }
        let payload = packet.logical_payload_with_config(&self.decode_config)?;
        if fnv1a64(&payload) != packet.base_hash {
            return Err(DeltaError::InvalidState("snapshot hash mismatch"));
        }
        let value: serde_json::Value = serde_json::from_slice(&payload)?;
        let migrated = migrations.migrate(packet.schema_hash, T::schema_hash(), value)?;
        let state: T = serde_json::from_value(migrated)?;
        self.sequence = Some(packet.sequence);
        self.state = Some(state.clone());
        self.replay.record(packet.sequence);
        Ok(GenericApplyResult::Applied {
            sequence: packet.sequence,
            state,
        })
    }

    pub fn state(&self) -> Option<&T> {
        self.state.as_ref()
    }
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }
    pub fn reset(&mut self) {
        self.sequence = None;
        self.state = None;
        self.replay.clear();
    }
}
