use crate::{error::DeltaError, packet::Packet};

/// Selects between raw and compressed snapshot or delta representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeMode {
    Snapshot,
    Delta,
    SnapshotZstd,
    DeltaZstd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodeDecision {
    pub mode: EncodeMode,
    pub snapshot_bytes: usize,
    pub delta_bytes: Option<usize>,
    pub snapshot_zstd_bytes: Option<usize>,
    pub delta_zstd_bytes: Option<usize>,
    pub selected_bytes: usize,
}

impl EncodeDecision {
    pub fn initial(
        snapshot_bytes: usize,
        snapshot_zstd_bytes: Option<usize>,
        mode: EncodeMode,
        selected_bytes: usize,
    ) -> Self {
        Self {
            mode,
            snapshot_bytes,
            delta_bytes: None,
            snapshot_zstd_bytes,
            delta_zstd_bytes: None,
            selected_bytes,
        }
    }
}

/// Adaptive codec selection policy.
///
/// Evaluates raw and compressed snapshot and delta candidates, then selects the
/// representation with the lowest configured cost.
///
/// A small penalty may be applied to compressed candidates to trade bandwidth
/// savings for lower CPU usage and latency.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptivePolicy {
    pub min_delta_savings_bytes: usize,
    pub max_delta_ratio: f64,
    pub enable_zstd: bool,
    pub zstd_level: i32,
    pub min_compression_savings_bytes: usize,
    pub compression_cpu_penalty_bytes: usize,
}

impl Default for AdaptivePolicy {
    fn default() -> Self {
        Self {
            min_delta_savings_bytes: 1,
            max_delta_ratio: 0.98,
            enable_zstd: cfg!(feature = "zstd-compression"),
            zstd_level: 1,
            min_compression_savings_bytes: 8,
            compression_cpu_penalty_bytes: 0,
        }
    }
}

impl AdaptivePolicy {
    fn score(&self, packet: &Packet) -> usize {
        packet
            .encoded_len()
            .saturating_add(if packet.is_compressed() {
                self.compression_cpu_penalty_bytes
            } else {
                0
            })
    }

    pub fn select_initial(&self, snapshot: Packet) -> Result<(Packet, EncodeDecision), DeltaError> {
        let raw_bytes = snapshot.encoded_len();
        let compressed = if self.enable_zstd {
            snapshot.zstd_candidate(self.zstd_level, self.min_compression_savings_bytes)?
        } else {
            snapshot.clone()
        };
        let compressed_bytes = compressed.encoded_len();
        let use_compressed = self.enable_zstd
            && compressed.is_compressed()
            && self.score(&compressed) < self.score(&snapshot);
        if use_compressed {
            Ok((
                compressed,
                EncodeDecision::initial(
                    raw_bytes,
                    Some(compressed_bytes),
                    EncodeMode::SnapshotZstd,
                    compressed_bytes,
                ),
            ))
        } else {
            Ok((
                snapshot,
                EncodeDecision::initial(
                    raw_bytes,
                    Some(compressed_bytes),
                    EncodeMode::Snapshot,
                    raw_bytes,
                ),
            ))
        }
    }

    pub fn select(
        &self,
        snapshot: Packet,
        delta: Packet,
    ) -> Result<(Packet, EncodeDecision), DeltaError> {
        let snapshot_bytes = snapshot.encoded_len();
        let delta_bytes = delta.encoded_len();

        let delta_allowed = snapshot_bytes > 0
            && (delta_bytes as f64 / snapshot_bytes as f64) <= self.max_delta_ratio
            && delta_bytes.saturating_add(self.min_delta_savings_bytes) <= snapshot_bytes;

        let snapshot_zstd = if self.enable_zstd {
            snapshot.zstd_candidate(self.zstd_level, self.min_compression_savings_bytes)?
        } else {
            snapshot.clone()
        };
        let delta_zstd = if self.enable_zstd {
            delta.zstd_candidate(self.zstd_level, self.min_compression_savings_bytes)?
        } else {
            delta.clone()
        };
        let snapshot_zstd_bytes = snapshot_zstd.encoded_len();
        let delta_zstd_bytes = delta_zstd.encoded_len();

        let mut best_packet = snapshot.clone();
        let mut best_mode = EncodeMode::Snapshot;
        let mut best_score = self.score(&snapshot);

        if self.enable_zstd
            && snapshot_zstd.is_compressed()
            && self.score(&snapshot_zstd) < best_score
        {
            best_score = self.score(&snapshot_zstd);
            best_packet = snapshot_zstd.clone();
            best_mode = EncodeMode::SnapshotZstd;
        }

        if delta_allowed && self.score(&delta) < best_score {
            best_score = self.score(&delta);
            best_packet = delta.clone();
            best_mode = EncodeMode::Delta;
        }

        if self.enable_zstd
            && delta_allowed
            && delta_zstd.is_compressed()
            && self.score(&delta_zstd) < best_score
        {
            best_packet = delta_zstd.clone();
            best_mode = EncodeMode::DeltaZstd;
        }

        let selected_bytes = best_packet.encoded_len();
        Ok((
            best_packet,
            EncodeDecision {
                mode: best_mode,
                snapshot_bytes,
                delta_bytes: Some(delta_bytes),
                snapshot_zstd_bytes: Some(snapshot_zstd_bytes),
                delta_zstd_bytes: Some(delta_zstd_bytes),
                selected_bytes,
            },
        ))
    }
}
