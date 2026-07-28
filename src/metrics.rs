use std::sync::{Arc, Mutex};

use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Metrics {
    pub updates: u64,
    pub full_json_bytes: u64,
    pub wire_bytes: u64,
    pub delta_packets: u64,
    pub snapshot_packets: u64,
    pub compressed_packets: u64,
    pub resyncs: u64,
    pub duplicates: u64,
    pub checksum_failures: u64,
    pub bytes_avoided: u64,
}

impl Metrics {
    pub fn reduction_percent(&self) -> f64 {
        if self.full_json_bytes == 0 {
            0.0
        } else {
            (1.0 - self.wire_bytes as f64 / self.full_json_bytes as f64) * 100.0
        }
    }
}

/// Cheap shared metrics sink for adapters, demos and production integration.
#[derive(Debug, Clone, Default)]
pub struct SharedMetrics(Arc<Mutex<Metrics>>);

impl SharedMetrics {
    pub fn record_update(
        &self,
        full_bytes: usize,
        wire_bytes: usize,
        is_delta: bool,
        is_compressed: bool,
    ) {
        if let Ok(mut m) = self.0.lock() {
            m.updates = m.updates.saturating_add(1);
            m.full_json_bytes = m.full_json_bytes.saturating_add(full_bytes as u64);
            m.wire_bytes = m.wire_bytes.saturating_add(wire_bytes as u64);
            m.bytes_avoided = m
                .bytes_avoided
                .saturating_add(full_bytes.saturating_sub(wire_bytes) as u64);
            if is_delta {
                m.delta_packets = m.delta_packets.saturating_add(1);
            } else {
                m.snapshot_packets = m.snapshot_packets.saturating_add(1);
            }
            if is_compressed {
                m.compressed_packets = m.compressed_packets.saturating_add(1);
            }
        }
    }

    pub fn record_resync(&self) {
        if let Ok(mut m) = self.0.lock() {
            m.resyncs = m.resyncs.saturating_add(1);
        }
    }

    pub fn record_duplicate(&self) {
        if let Ok(mut m) = self.0.lock() {
            m.duplicates = m.duplicates.saturating_add(1);
        }
    }

    pub fn snapshot(&self) -> Metrics {
        self.0.lock().map(|m| m.clone()).unwrap_or_default()
    }
}
