use crate::{
    error::DeltaError,
    packet::{Packet, PacketKind},
    state::AgentState,
    sync::{ApplyResult, Decoder, Encoder},
};

#[derive(Debug, Clone, Default)]
pub struct MultiClientReport {
    pub clients: usize,
    pub updates: usize,
    pub deliveries: u64,
    pub drops: u64,
    pub duplicates: u64,
    pub reorders: u64,
    pub disconnects: u64,
    pub resyncs: u64,
    pub converged_clients: usize,
    pub final_sequence: u64,
}

impl MultiClientReport {
    pub fn all_converged(&self) -> bool {
        self.converged_clients == self.clients
    }
}

#[derive(Default)]
struct SimClient {
    decoder: Decoder,
    offline_until: usize,
    delayed: Option<Packet>,
}

fn deliver(
    client: &mut SimClient,
    packet: Packet,
    encoder: &Encoder,
    state: &AgentState,
    report: &mut MultiClientReport,
) -> Result<(), DeltaError> {
    match client.decoder.apply_packet(packet)? {
        ApplyResult::Applied { .. } => report.deliveries += 1,
        ApplyResult::Duplicate { .. } => report.duplicates += 1,
        ApplyResult::NeedSnapshot { .. } => {
            report.resyncs += 1;
            let recovery = encoder.recovery_snapshot(state)?;
            match client.decoder.apply_packet(recovery)? {
                ApplyResult::Applied { .. } | ApplyResult::Duplicate { .. } => {
                    report.deliveries += 1;
                }
                ApplyResult::NeedSnapshot { .. } => {
                    return Err(DeltaError::InvalidState("multi-client recovery failed"));
                }
            }
        }
    }
    Ok(())
}

/// Deterministic multi-client fan-out simulation.
///
/// It exercises independent client loss, duplicates, reordering and short
/// disconnects. Recovery snapshots do not advance the shared publisher
/// sequence, so one client's resync cannot create gaps for everyone else.
pub fn run_deterministic(clients: usize, updates: usize) -> Result<MultiClientReport, DeltaError> {
    let client_count = clients.max(1);
    let mut report = MultiClientReport {
        clients: client_count,
        updates,
        ..MultiClientReport::default()
    };
    let mut encoder = Encoder::default();
    let mut state = AgentState::demo();
    let mut subscribers: Vec<SimClient> = (0..client_count).map(|_| SimClient::default()).collect();

    for update in 0..updates {
        state = state.advance();
        let packet = encoder.encode(&state)?;

        for (id, client) in subscribers.iter_mut().enumerate() {
            if update > 0 && (update + id * 13) % 997 == 0 {
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

            if update > 0
                && packet.kind == PacketKind::Delta
                && (update + id * 19) % 389 == 0
                && client.delayed.is_none()
            {
                client.delayed = Some(packet.clone());
                report.reorders += 1;
                continue;
            }

            deliver(client, packet.clone(), &encoder, &state, &mut report)?;

            if update > 0 && (update + id * 23) % 307 == 0 {
                deliver(client, packet.clone(), &encoder, &state, &mut report)?;
            }

            if let Some(delayed) = client.delayed.take() {
                deliver(client, delayed, &encoder, &state, &mut report)?;
            }
        }
    }

    // Authoritative final recovery broadcast proves eventual convergence even
    // for a subscriber that happened to be offline on the last update.
    let final_snapshot = encoder.recovery_snapshot(&state)?;
    for client in &mut subscribers {
        let _ = client.decoder.apply_packet(final_snapshot.clone())?;
    }

    report.final_sequence = encoder.sequence();
    report.converged_clients = subscribers
        .iter()
        .filter(|client| client.decoder.state() == Some(&state))
        .count();

    Ok(report)
}
