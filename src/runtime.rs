#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLimits {
    pub max_history_packets: usize,
    pub max_history_bytes: usize,
    pub max_pending_reorder: usize,
    pub max_client_lag: u64,
    pub max_recoveries_per_window: u32,
    pub max_encode_work_bytes: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_history_packets: 512,
            max_history_bytes: 16 * 1024 * 1024,
            max_pending_reorder: 128,
            max_client_lag: 10_000,
            max_recoveries_per_window: 64,
            max_encode_work_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeMetrics {
    pub encoded_packets: u64,
    pub snapshots: u64,
    pub deltas: u64,
    pub compressed_packets: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub recovery_replays: u64,
    pub recovery_snapshots: u64,
    pub reorder_buffered: u64,
    pub backpressure_events: u64,
    pub selector_candidates: u64,
}

impl RuntimeMetrics {
    pub fn compression_ratio(&self) -> f64 {
        if self.input_bytes == 0 {
            1.0
        } else {
            self.output_bytes as f64 / self.input_bytes as f64
        }
    }
    pub fn saving_percent(&self) -> f64 {
        (1.0 - self.compression_ratio()) * 100.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureDecision {
    Accept,
    SnapshotAndCatchUp,
    DropClient,
}

pub fn backpressure_decision(
    local_sequence: Option<u64>,
    publisher_sequence: u64,
    limits: RuntimeLimits,
) -> BackpressureDecision {
    let Some(local) = local_sequence else {
        return BackpressureDecision::SnapshotAndCatchUp;
    };
    let lag = publisher_sequence.saturating_sub(local);
    if lag <= limits.max_client_lag {
        BackpressureDecision::Accept
    } else if lag <= limits.max_client_lag.saturating_mul(10) {
        BackpressureDecision::SnapshotAndCatchUp
    } else {
        BackpressureDecision::DropClient
    }
}
