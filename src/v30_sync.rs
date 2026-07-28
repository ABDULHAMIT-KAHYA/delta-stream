use crate::{
    error::DeltaError,
    fast_selector::{ChangeProfile, SelectorPolicy, StrategyAdvisor},
    packet::Packet,
    runtime::RuntimeMetrics,
    smart_delta::{self, SmartDeltaKind, SmartDeltaPolicy},
    sync::fnv1a64,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V30Policy {
    pub smart_delta: SmartDeltaPolicy,
    pub selector: SelectorPolicy,
    pub enable_zstd: bool,
    pub zstd_level: i32,
    pub min_zstd_payload: usize,
    pub min_compression_savings: usize,
}
impl Default for V30Policy {
    fn default() -> Self {
        Self {
            smart_delta: SmartDeltaPolicy::default(),
            selector: SelectorPolicy::default(),
            enable_zstd: cfg!(feature = "zstd-compression"),
            zstd_level: 1,
            min_zstd_payload: 256,
            min_compression_savings: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V30EncodeMode {
    Snapshot,
    SnapshotZstd,
    Delta(SmartDeltaKind),
    DeltaZstd(SmartDeltaKind),
}

#[derive(Debug, Clone, PartialEq)]
pub struct V30Decision {
    pub mode: V30EncodeMode,
    pub profile: Option<ChangeProfile>,
    pub candidates_considered: usize,
    pub selected_bytes: usize,
    pub snapshot_bytes: usize,
}

#[derive(Debug)]
pub struct FastByteStateEncoder {
    sequence: u64,
    previous: Option<Vec<u8>>,
    schema_hash: u64,
    policy: V30Policy,
    advisor: StrategyAdvisor,
    metrics: RuntimeMetrics,
    last_decision: Option<V30Decision>,
}

impl FastByteStateEncoder {
    pub fn new(schema_name: &str) -> Self {
        Self::with_policy(schema_name, V30Policy::default())
    }
    pub fn with_policy(schema_name: &str, policy: V30Policy) -> Self {
        Self {
            sequence: 0,
            previous: None,
            schema_hash: fnv1a64(schema_name.as_bytes()),
            advisor: StrategyAdvisor::new(policy.selector),
            policy,
            metrics: RuntimeMetrics::default(),
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
        let mut best = snapshot;
        let mut mode = V30EncodeMode::Snapshot;
        let mut considered = 1usize;
        let mut profile_out = None;

        if let Some(previous) = &self.previous {
            let profile = ChangeProfile::analyze(previous, current);
            profile_out = Some(profile);
            let shortlist = self.advisor.shortlist(profile);
            let previous_hash = fnv1a64(previous);
            for kind in shortlist {
                let candidate = smart_delta::encode_candidate(
                    kind,
                    previous,
                    current,
                    self.policy.smart_delta,
                )?;
                let delta = Packet::delta(
                    sequence,
                    sequence - 1,
                    previous_hash,
                    self.schema_hash,
                    candidate.payload,
                );
                considered += 1;
                if delta.encoded_len() < best.encoded_len() {
                    best = delta.clone();
                    mode = V30EncodeMode::Delta(kind);
                }
                if self.policy.enable_zstd
                    && self
                        .advisor
                        .should_compress(delta.payload.len(), self.policy.min_zstd_payload)
                {
                    let compressed = delta.zstd_candidate(
                        self.policy.zstd_level,
                        self.policy.min_compression_savings,
                    )?;
                    considered += 1;
                    let won = compressed.is_compressed()
                        && compressed.encoded_len() < delta.encoded_len();
                    self.advisor.observe_compression(won);
                    if compressed.is_compressed() && compressed.encoded_len() < best.encoded_len() {
                        best = compressed;
                        mode = V30EncodeMode::DeltaZstd(kind);
                    }
                }
            }
            match mode {
                V30EncodeMode::Delta(k) | V30EncodeMode::DeltaZstd(k) => {
                    self.advisor.observe_winner(k)
                }
                _ => self.advisor.observe_no_delta(),
            }
        } else if self.policy.enable_zstd && current.len() >= self.policy.min_zstd_payload {
            let compressed =
                best.zstd_candidate(self.policy.zstd_level, self.policy.min_compression_savings)?;
            considered += 1;
            if compressed.is_compressed() && compressed.encoded_len() < best.encoded_len() {
                best = compressed;
                mode = V30EncodeMode::SnapshotZstd;
            }
        }

        self.metrics.encoded_packets = self.metrics.encoded_packets.saturating_add(1);
        self.metrics.input_bytes = self
            .metrics
            .input_bytes
            .saturating_add(current.len() as u64);
        self.metrics.output_bytes = self
            .metrics
            .output_bytes
            .saturating_add(best.encoded_len() as u64);
        self.metrics.selector_candidates = self
            .metrics
            .selector_candidates
            .saturating_add(considered as u64);
        match mode {
            V30EncodeMode::Snapshot | V30EncodeMode::SnapshotZstd => self.metrics.snapshots += 1,
            V30EncodeMode::Delta(_) | V30EncodeMode::DeltaZstd(_) => self.metrics.deltas += 1,
        }
        if matches!(
            mode,
            V30EncodeMode::SnapshotZstd | V30EncodeMode::DeltaZstd(_)
        ) {
            self.metrics.compressed_packets += 1;
        }

        self.previous = Some(current.to_vec());
        self.last_decision = Some(V30Decision {
            mode,
            profile: profile_out,
            candidates_considered: considered,
            selected_bytes: best.encoded_len(),
            snapshot_bytes,
        });
        Ok(best)
    }

    pub fn recovery_snapshot(&self, current: &[u8]) -> Result<Packet, DeltaError> {
        Ok(Packet::snapshot(
            self.sequence,
            fnv1a64(current),
            self.schema_hash,
            current.to_vec(),
        ))
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn metrics(&self) -> &RuntimeMetrics {
        &self.metrics
    }
    pub fn last_decision(&self) -> Option<&V30Decision> {
        self.last_decision.as_ref()
    }
}
