use serde::{Deserialize, Serialize};

use crate::packet::{MAX_LOGICAL_PAYLOAD, MAX_WIRE_PAYLOAD, MIN_WIRE_VERSION, WIRE_VERSION};

pub const LIBRARY_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROTOCOL_MIN: u8 = MIN_WIRE_VERSION;
pub const PROTOCOL_MAX: u8 = WIRE_VERSION;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Capabilities {
    pub library_version: String,
    pub protocol_min: u8,
    pub protocol_max: u8,
    pub adaptive_delta_snapshot: bool,
    pub adaptive_four_way_codec: bool,
    pub automatic_resync: bool,
    pub stable_sequence_recovery: bool,
    pub duplicate_and_stale_suppression: bool,
    pub crc32: bool,
    pub schema_hash: bool,
    pub zstd: bool,
    pub max_wire_payload: usize,
    pub max_logical_payload: usize,
    pub smart_delta_sparse: bool,
    pub smart_delta_ranges: bool,
    pub smart_delta_xor: bool,
    pub smart_delta_splice: bool,
    pub smart_delta_chunks: bool,
    pub adaptive_self_tuning: bool,
    pub recovery_replay_planner: bool,
    pub bounded_reorder_buffer: bool,
    pub resync_storm_snapshot_reuse: bool,
}

impl Capabilities {
    pub fn local() -> Self {
        Self {
            library_version: LIBRARY_VERSION.to_string(),
            protocol_min: PROTOCOL_MIN,
            protocol_max: PROTOCOL_MAX,
            adaptive_delta_snapshot: true,
            adaptive_four_way_codec: true,
            automatic_resync: true,
            stable_sequence_recovery: true,
            duplicate_and_stale_suppression: true,
            crc32: true,
            schema_hash: true,
            zstd: true,
            max_wire_payload: MAX_WIRE_PAYLOAD,
            max_logical_payload: MAX_LOGICAL_PAYLOAD,
            smart_delta_sparse: true,
            smart_delta_ranges: true,
            smart_delta_xor: true,
            smart_delta_splice: true,
            smart_delta_chunks: true,
            adaptive_self_tuning: true,
            recovery_replay_planner: true,
            bounded_reorder_buffer: true,
            resync_storm_snapshot_reuse: true,
        }
    }
}
