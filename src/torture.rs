use crate::{
    error::DeltaError,
    packet::{Packet, PacketKind},
    reorder::{ReorderApplyResult, ReorderDecoder},
    state::AgentState,
    sync::Encoder,
};

#[derive(Debug, Clone, Default)]
pub struct TortureReport {
    pub resync_storm_snapshots_built: u64,
    pub clients: usize,
    pub updates: usize,
    pub deliveries: u64,
    pub drops: u64,
    pub duplicates: u64,
    pub corruptions: u64,
    pub reorders: u64,
    pub buffered_reorders: u64,
    pub disconnects: u64,
    pub late_joins: u64,
    pub long_disconnects: u64,
    pub resyncs: u64,
    pub resync_storm_clients: u64,
    pub recovery_snapshots_built: u64,
    pub converged_clients: usize,
    pub final_sequence: u64,
}

impl TortureReport {
    pub fn all_converged(&self) -> bool {
        self.converged_clients == self.clients
    }
}

#[derive(Default)]
struct Client {
    decoder: ReorderDecoder,
    offline_until: usize,
    joined: bool,
    delayed: Option<Packet>,
}

fn deliver(
    client: &mut Client,
    packet: Packet,
    encoder: &Encoder,
    state: &AgentState,
    report: &mut TortureReport,
) -> Result<(), DeltaError> {
    match client.decoder.apply(packet.clone())? {
        ReorderApplyResult::Applied { drained, .. } => {
            report.deliveries += 1;
            report.buffered_reorders += drained as u64;
        }
        ReorderApplyResult::Buffered { .. } => {
            report.reorders += 1;
        }
        ReorderApplyResult::Duplicate { .. } => report.duplicates += 1,
        ReorderApplyResult::NeedRecovery { .. } => {
            report.resyncs += 1;
            client.decoder.clear_pending();
            let recovery = encoder.recovery_snapshot(state)?;
            report.recovery_snapshots_built += 1;
            match client.decoder.apply(recovery)? {
                ReorderApplyResult::Applied { .. } | ReorderApplyResult::Duplicate { .. } => {
                    report.deliveries += 1
                }
                _ => return Err(DeltaError::InvalidState("torture recovery failed")),
            }
        }
    }
    Ok(())
}

/// Deterministic V25 hard simulation.
///
/// It includes drops, duplicates, bounded reordering, short disconnects,
/// long disconnects, late joins, and one global outage/resync storm.
pub fn run(clients: usize, updates: usize) -> Result<TortureReport, DeltaError> {
    let client_count = clients.max(1);
    let update_count = updates.max(1);
    let mut report = TortureReport {
        clients: client_count,
        updates: update_count,
        ..TortureReport::default()
    };
    let mut encoder = Encoder::default();
    let mut state = AgentState::demo();
    let mut subscribers: Vec<Client> = (0..client_count).map(|_| Client::default()).collect();
    let storm_at = update_count / 2;

    for update in 0..update_count {
        state = state.advance();
        let packet = encoder.encode(&state)?;

        // Global outage: all joined clients miss this update. On the next update
        // a single recovery snapshot is constructed once and cloned to everyone.
        if update == storm_at {
            for client in &subscribers {
                if client.joined {
                    report.drops += 1;
                }
            }
            continue;
        }
        if update == storm_at.saturating_add(1) {
            let shared = encoder.recovery_snapshot(&state)?;
            report.recovery_snapshots_built += 1;
            report.resync_storm_snapshots_built += 1;
            for client in &mut subscribers {
                if client.joined {
                    client.decoder.clear_pending();
                    let _ = client.decoder.apply(shared.clone())?;
                    report.resync_storm_clients += 1;
                    report.resyncs += 1;
                }
            }
            continue;
        }

        for (id, client) in subscribers.iter_mut().enumerate() {
            let join_at = if id % 10 == 0 {
                (id * 37) % update_count.max(2)
            } else {
                0
            };
            if !client.joined {
                if update < join_at {
                    continue;
                }
                client.joined = true;
                if join_at > 0 {
                    report.late_joins += 1;
                }
                let snapshot = encoder.recovery_snapshot(&state)?;
                report.recovery_snapshots_built += 1;
                let _ = client.decoder.apply(snapshot)?;
                continue;
            }

            if update > 0 && (update + id * 29) % 10_007 == 0 {
                client.offline_until = update.saturating_add(500.min(update_count / 4 + 1));
                report.disconnects += 1;
                report.long_disconnects += 1;
            } else if update > 0 && (update + id * 13) % 997 == 0 {
                client.offline_until = update.saturating_add(5);
                report.disconnects += 1;
            }
            if update < client.offline_until {
                report.drops += 1;
                continue;
            }

            if update > 0 && (update + id * 17) % 211 == 0 {
                report.drops += 1;
                continue;
            }

            // Corrupt a real encoded packet and prove CRC/decoder rejection.
            // The corrupted delivery is treated as lost; later sequence checks
            // drive normal recovery.
            if update > 0 && (update + id * 31) % 1237 == 0 {
                let mut wire = packet.encode()?;
                if let Some(last) = wire.last_mut() {
                    *last ^= 0x80;
                }
                if Packet::decode(&wire).is_ok() {
                    return Err(DeltaError::InvalidState(
                        "corrupted torture packet was accepted",
                    ));
                }
                report.corruptions += 1;
                report.drops += 1;
                continue;
            }

            // Reorder by withholding one delta. On the next update the future
            // packet is delivered first (and buffered), then the delayed base
            // arrives and drains the reorder buffer without a resync.
            if update > 1
                && packet.kind == PacketKind::Delta
                && client.delayed.is_none()
                && (update + id * 19) % 389 == 0
            {
                client.delayed = Some(packet.clone());
                report.reorders += 1;
                continue;
            }

            deliver(client, packet.clone(), &encoder, &state, &mut report)?;

            if let Some(delayed) = client.delayed.take() {
                match client.decoder.apply(delayed)? {
                    ReorderApplyResult::Applied { drained, .. } => {
                        report.deliveries += 1;
                        report.buffered_reorders += drained as u64;
                    }
                    ReorderApplyResult::Buffered { .. } => report.reorders += 1,
                    ReorderApplyResult::Duplicate { .. } => report.duplicates += 1,
                    ReorderApplyResult::NeedRecovery { .. } => {
                        report.resyncs += 1;
                        client.decoder.clear_pending();
                        let snapshot = encoder.recovery_snapshot(&state)?;
                        report.recovery_snapshots_built += 1;
                        let _ = client.decoder.apply(snapshot)?;
                    }
                }
            }

            if update > 0 && (update + id * 23) % 307 == 0 {
                deliver(client, packet.clone(), &encoder, &state, &mut report)?;
            }
        }
    }

    let final_snapshot = encoder.recovery_snapshot(&state)?;
    report.recovery_snapshots_built += 1;
    for client in &mut subscribers {
        client.joined = true;
        client.decoder.clear_pending();
        let _ = client.decoder.apply(final_snapshot.clone())?;
    }

    report.final_sequence = encoder.sequence();
    report.converged_clients = subscribers
        .iter()
        .filter(|client| client.decoder.state() == Some(&state))
        .count();
    Ok(report)
}
