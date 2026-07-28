use crate::{
    error::DeltaError,
    packet::{Packet, PacketKind},
    smart_delta::{self, AdaptiveTuner, SmartDeltaKind, SmartDeltaPolicy},
    sync::fnv1a64,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteEncodeMode {
    Snapshot,
    SnapshotZstd,
    Delta(SmartDeltaKind),
    DeltaZstd(SmartDeltaKind),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteEncodeDecision {
    pub mode: ByteEncodeMode,
    pub selected_bytes: usize,
    pub snapshot_bytes: usize,
    pub candidates_considered: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V25Policy {
    pub smart_delta: SmartDeltaPolicy,
    pub enable_zstd: bool,
    pub zstd_level: i32,
    pub min_compression_savings: usize,
    pub compression_cpu_penalty_bytes: usize,
}

impl Default for V25Policy {
    fn default() -> Self {
        Self {
            smart_delta: SmartDeltaPolicy::default(),
            enable_zstd: cfg!(feature = "zstd-compression"),
            zstd_level: 1,
            min_compression_savings: 8,
            compression_cpu_penalty_bytes: 0,
        }
    }
}

#[derive(Debug)]
pub struct ByteStateEncoder {
    sequence: u64,
    previous: Option<Vec<u8>>,
    schema_hash: u64,
    policy: V25Policy,
    tuner: AdaptiveTuner,
    last_decision: Option<ByteEncodeDecision>,
}

impl ByteStateEncoder {
    pub fn new(schema_name: &str) -> Self {
        Self::with_policy(schema_name, V25Policy::default())
    }

    pub fn with_policy(schema_name: &str, policy: V25Policy) -> Self {
        Self {
            sequence: 0,
            previous: None,
            schema_hash: fnv1a64(schema_name.as_bytes()),
            policy,
            tuner: AdaptiveTuner::new(),
            last_decision: None,
        }
    }

    pub fn encode(&mut self, current: &[u8]) -> Result<Packet, DeltaError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(DeltaError::InvalidState("sequence exhausted"))?;
        let sequence = self.sequence;
        let snapshot = Packet::snapshot(
            sequence,
            fnv1a64(current),
            self.schema_hash,
            current.to_vec(),
        );
        let snapshot_bytes = snapshot.encoded_len();
        let mut best = snapshot.clone();
        let mut mode = ByteEncodeMode::Snapshot;
        let mut best_score = snapshot_bytes;
        let mut considered = 1usize;

        if self.policy.enable_zstd && self.tuner.should_try_zstd(snapshot.payload.len()) {
            let compressed = snapshot
                .zstd_candidate(self.policy.zstd_level, self.policy.min_compression_savings)?;
            considered += 1;
            self.tuner
                .observe_compression(snapshot.encoded_len(), compressed.encoded_len());
            let score = compressed
                .encoded_len()
                .saturating_add(if compressed.is_compressed() {
                    self.policy.compression_cpu_penalty_bytes
                } else {
                    0
                });
            if compressed.is_compressed() && score < best_score {
                best_score = score;
                best = compressed;
                mode = ByteEncodeMode::SnapshotZstd;
            }
        }

        if let Some(previous) = &self.previous {
            for candidate in
                smart_delta::encode_candidates(previous, current, self.policy.smart_delta)?
            {
                let delta = Packet::delta(
                    sequence,
                    sequence - 1,
                    fnv1a64(previous),
                    self.schema_hash,
                    candidate.payload,
                );
                considered += 1;
                if delta.encoded_len() < best_score {
                    best_score = delta.encoded_len();
                    best = delta.clone();
                    mode = ByteEncodeMode::Delta(candidate.kind);
                }

                if self.policy.enable_zstd && self.tuner.should_try_zstd(delta.payload.len()) {
                    let compressed = delta.zstd_candidate(
                        self.policy.zstd_level,
                        self.policy.min_compression_savings,
                    )?;
                    considered += 1;
                    self.tuner
                        .observe_compression(delta.encoded_len(), compressed.encoded_len());
                    let score =
                        compressed
                            .encoded_len()
                            .saturating_add(if compressed.is_compressed() {
                                self.policy.compression_cpu_penalty_bytes
                            } else {
                                0
                            });
                    if compressed.is_compressed() && score < best_score {
                        best_score = score;
                        best = compressed;
                        mode = ByteEncodeMode::DeltaZstd(candidate.kind);
                    }
                }
            }
        }

        self.previous = Some(current.to_vec());
        self.last_decision = Some(ByteEncodeDecision {
            mode,
            selected_bytes: best.encoded_len(),
            snapshot_bytes,
            candidates_considered: considered,
        });
        Ok(best)
    }

    pub fn recovery_snapshot(&self, current: &[u8]) -> Result<Packet, DeltaError> {
        let snapshot = Packet::snapshot(
            self.sequence,
            fnv1a64(current),
            self.schema_hash,
            current.to_vec(),
        );
        if self.policy.enable_zstd && self.tuner.should_try_zstd(snapshot.payload.len()) {
            snapshot.zstd_candidate(self.policy.zstd_level, self.policy.min_compression_savings)
        } else {
            Ok(snapshot)
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn last_decision(&self) -> Option<&ByteEncodeDecision> {
        self.last_decision.as_ref()
    }
    pub fn tuner(&self) -> AdaptiveTuner {
        self.tuner
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteApplyResult {
    Applied {
        sequence: u64,
        state: Vec<u8>,
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
pub struct ByteStateDecoder {
    sequence: Option<u64>,
    state: Option<Vec<u8>>,
    schema_hash: u64,
}

impl ByteStateDecoder {
    pub fn new(schema_name: &str) -> Self {
        Self {
            sequence: None,
            state: None,
            schema_hash: fnv1a64(schema_name.as_bytes()),
        }
    }

    pub fn apply(&mut self, packet: Packet) -> Result<ByteApplyResult, DeltaError> {
        if packet.schema_hash != self.schema_hash {
            return Err(DeltaError::SchemaMismatch {
                expected: self.schema_hash,
                received: packet.schema_hash,
            });
        }
        if self.sequence.is_some_and(|seq| packet.sequence <= seq) {
            return Ok(ByteApplyResult::Duplicate {
                sequence: packet.sequence,
            });
        }
        let payload = packet.logical_payload()?;
        let next = match packet.kind {
            PacketKind::Snapshot => {
                if fnv1a64(&payload) != packet.base_hash {
                    return Err(DeltaError::InvalidState("byte snapshot hash mismatch"));
                }
                payload
            }
            PacketKind::Delta => {
                if self.sequence != Some(packet.base_sequence) {
                    return Ok(ByteApplyResult::NeedRecovery {
                        local_sequence: self.sequence,
                        required_sequence: packet.base_sequence,
                    });
                }
                let base = self
                    .state
                    .as_ref()
                    .ok_or(DeltaError::InvalidState("byte decoder has no base state"))?;
                if fnv1a64(base) != packet.base_hash {
                    return Ok(ByteApplyResult::NeedRecovery {
                        local_sequence: self.sequence,
                        required_sequence: packet.base_sequence,
                    });
                }
                smart_delta::apply(base, &payload)?
            }
        };
        self.sequence = Some(packet.sequence);
        self.state = Some(next.clone());
        Ok(ByteApplyResult::Applied {
            sequence: packet.sequence,
            state: next,
        })
    }

    pub fn state(&self) -> Option<&[u8]> {
        self.state.as_deref()
    }
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }
    pub fn reset(&mut self) {
        self.sequence = None;
        self.state = None;
    }
}
