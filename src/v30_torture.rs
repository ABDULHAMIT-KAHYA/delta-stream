use crate::{
    byte_sync::{ByteApplyResult, ByteStateDecoder},
    error::DeltaError,
    packet::Packet,
    v30_sync::FastByteStateEncoder,
};

#[derive(Debug, Clone, Default)]
pub struct V30TortureReport {
    pub clients: usize,
    pub updates: usize,
    pub drops: u64,
    pub duplicates: u64,
    pub corruptions: u64,
    pub reorders: u64,
    pub recoveries: u64,
    pub shared_recovery_snapshots: u64,
    pub late_joins: u64,
    pub converged: usize,
}
impl V30TortureReport {
    pub fn all_converged(&self) -> bool {
        self.converged == self.clients
    }
}

struct Client {
    decoder: ByteStateDecoder,
    delayed: Option<Packet>,
    joined: bool,
}

pub fn run(clients: usize, updates: usize) -> Result<V30TortureReport, DeltaError> {
    let clients = clients.max(1);
    let updates = updates.max(2);
    let mut report = V30TortureReport {
        clients,
        updates,
        ..Default::default()
    };
    let mut encoder = FastByteStateEncoder::new("v30/torture");
    let mut state = vec![0x5Au8; 4096];
    let mut peers: Vec<Client> = (0..clients)
        .map(|_| Client {
            decoder: ByteStateDecoder::new("v30/torture"),
            delayed: None,
            joined: false,
        })
        .collect();

    for update in 1..=updates {
        if update > 1 {
            let i = (update * 97) % state.len();
            state[i] ^= (update as u8).wrapping_add(1);
        }
        let packet = encoder.encode(&state)?;
        let mut need_recovery = Vec::new();

        for (id, peer) in peers.iter_mut().enumerate() {
            let join_at = 1 + (id % updates.min(97));
            if !peer.joined {
                if update < join_at {
                    continue;
                }
                peer.joined = true;
                if join_at > 1 {
                    report.late_joins += 1;
                    need_recovery.push(id);
                    continue;
                }
            }

            // deterministic short disconnect/drop
            if (update + id * 17) % 211 == 0 {
                report.drops += 1;
                continue;
            }

            // one-packet reordering: hold then deliver after the next packet
            if (update + id * 13) % 509 == 0 && peer.delayed.is_none() {
                peer.delayed = Some(packet.clone());
                report.reorders += 1;
                continue;
            }

            // packet corruption is caught by wire decode before state application
            if (update + id * 19) % 997 == 0 {
                let mut wire = packet.encode()?;
                if let Some(last) = wire.last_mut() {
                    *last ^= 0xA5;
                }
                report.corruptions += 1;
                if Packet::decode(&wire).is_err() {
                    need_recovery.push(id);
                    continue;
                }
            }

            match peer.decoder.apply(packet.clone())? {
                ByteApplyResult::Applied { .. } | ByteApplyResult::Duplicate { .. } => {}
                ByteApplyResult::NeedRecovery { .. } => {
                    report.recoveries += 1;
                    need_recovery.push(id);
                }
            }

            if (update + id * 23) % 701 == 0 {
                report.duplicates += 1;
                let _ = peer.decoder.apply(packet.clone())?;
            }

            if let Some(delayed) = peer.delayed.take() {
                // Delayed old packet must not roll state back.
                let _ = peer.decoder.apply(delayed)?;
            }
        }

        if !need_recovery.is_empty() {
            let shared = encoder.recovery_snapshot(&state)?;
            report.shared_recovery_snapshots += 1;
            need_recovery.sort_unstable();
            need_recovery.dedup();
            for id in need_recovery {
                match peers[id].decoder.apply(shared.clone())? {
                    ByteApplyResult::Applied { .. } | ByteApplyResult::Duplicate { .. } => {}
                    ByteApplyResult::NeedRecovery { .. } => {
                        return Err(DeltaError::InvalidState("V30 shared recovery failed"))
                    }
                }
            }
        }
    }

    let final_snapshot = encoder.recovery_snapshot(&state)?;
    for peer in &mut peers {
        peer.delayed = None;
        let _ = peer.decoder.apply(final_snapshot.clone())?;
        if peer.decoder.state() == Some(state.as_slice())
            && peer.decoder.sequence() == Some(encoder.sequence())
        {
            report.converged += 1;
        }
    }
    Ok(report)
}
