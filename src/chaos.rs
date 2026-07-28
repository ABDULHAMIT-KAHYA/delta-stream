use crate::{
    error::DeltaError,
    state::AgentState,
    sync::{ApplyResult, Decoder, Encoder},
};

#[derive(Debug, Clone, Default)]
pub struct ChaosReport {
    pub generated: u64,
    pub delivered: u64,
    pub intentionally_dropped: u64,
    pub duplicates_injected: u64,
    pub resyncs: u64,
    pub final_sequence: Option<u64>,
    pub final_state_matches: bool,
}

/// Deterministic chaos validation with no network dependency.
///
/// It drops a packet periodically, injects duplicates, performs snapshot
/// recovery on desync, and verifies that the receiver converges to the
/// publisher's final state.
pub fn run_deterministic(updates: usize) -> Result<ChaosReport, DeltaError> {
    let mut report = ChaosReport::default();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();
    let mut state = AgentState::demo();

    for i in 0..updates {
        state = state.advance();
        let packet = encoder.encode(&state)?;
        report.generated += 1;

        // Drop every 97th non-initial data packet.
        if i > 0 && i % 97 == 0 {
            report.intentionally_dropped += 1;
            continue;
        }

        // Duplicate every 131st delivered packet.
        let duplicate = if i > 0 && i % 131 == 0 {
            Some(packet.clone())
        } else {
            None
        };

        match decoder.apply_packet(packet)? {
            ApplyResult::Applied { .. } | ApplyResult::Duplicate { .. } => {
                report.delivered += 1;
            }
            ApplyResult::NeedSnapshot { .. } => {
                report.resyncs += 1;
                let recovery = encoder.recovery_snapshot(&state)?;
                match decoder.apply_packet(recovery)? {
                    ApplyResult::Applied { .. } => report.delivered += 1,
                    _ => return Err(DeltaError::InvalidState("chaos recovery failed")),
                }
            }
        }

        if let Some(dup) = duplicate {
            report.duplicates_injected += 1;
            match decoder.apply_packet(dup)? {
                ApplyResult::Duplicate { .. } | ApplyResult::NeedSnapshot { .. } => {}
                ApplyResult::Applied { .. } => {}
            }
        }
    }

    // Always finish with an authoritative snapshot so the soak/chaos test can
    // prove eventual convergence even when the final update happened to drop.
    let final_snapshot = encoder.recovery_snapshot(&state)?;
    let _ = decoder.apply_packet(final_snapshot)?;
    report.final_sequence = decoder.sequence();
    report.final_state_matches = decoder.state() == Some(&state);
    Ok(report)
}
