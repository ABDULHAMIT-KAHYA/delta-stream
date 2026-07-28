use crate::{
    error::DeltaError,
    packet::Packet,
    partial_repair::PartialRepair,
    recovery_history::{RecoveryHistory, RecoveryPlan},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V30RecoveryPlan {
    Replay(Vec<Packet>),
    Partial(PartialRepair),
    Snapshot(Packet),
}

impl V30RecoveryPlan {
    pub fn estimated_payload_bytes(&self) -> usize {
        match self {
            Self::Replay(packets) => packets.iter().map(Packet::encoded_len).sum(),
            Self::Partial(repair) => repair.payload_bytes(),
            Self::Snapshot(packet) => packet.encoded_len(),
        }
    }
}

pub fn plan_recovery(
    history: &RecoveryHistory,
    local_sequence: Option<u64>,
    receiver_state: Option<&[u8]>,
    authoritative_state: &[u8],
    snapshot: Packet,
    chunk_size: usize,
) -> Result<V30RecoveryPlan, DeltaError> {
    let history_plan = history.plan(local_sequence, snapshot.clone())?;
    let mut best = match history_plan {
        RecoveryPlan::Replay(p) => V30RecoveryPlan::Replay(p),
        RecoveryPlan::Snapshot(p) => V30RecoveryPlan::Snapshot(p),
    };
    if let Some(receiver) = receiver_state {
        let repair = PartialRepair::build(receiver, authoritative_state, chunk_size);
        if repair.payload_bytes() < best.estimated_payload_bytes() {
            best = V30RecoveryPlan::Partial(repair);
        }
    }
    Ok(best)
}
