use crate::{
    adaptive::{AdaptivePolicy, EncodeMode},
    chaos,
    error::DeltaError,
    multi_client,
    packet::Packet,
    state::AgentState,
    sync::{ApplyResult, Decoder, Encoder},
};

#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub checks: Vec<(&'static str, bool)>,
}

impl ValidationReport {
    pub fn all_passed(&self) -> bool {
        self.checks.iter().all(|(_, ok)| *ok)
    }
}

pub fn run_release_validation() -> Result<ValidationReport, DeltaError> {
    let mut checks = Vec::new();

    let initial = AgentState::demo();
    let next = initial.advance();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();
    let p1 = encoder.encode(&initial)?;
    let p2 = encoder.encode(&next)?;
    checks.push((
        "initial packet is snapshot",
        matches!(p1.kind, crate::PacketKind::Snapshot),
    ));
    checks.push(("packet roundtrip", Packet::decode(&p2.encode()?)? == p2));
    checks.push((
        "snapshot applies",
        matches!(decoder.apply_packet(p1)?, ApplyResult::Applied { .. }),
    ));
    checks.push((
        "next update applies",
        matches!(
            decoder.apply_packet(p2.clone())?,
            ApplyResult::Applied { .. }
        ),
    ));
    checks.push((
        "duplicate suppressed",
        matches!(decoder.apply_packet(p2)?, ApplyResult::Duplicate { .. }),
    ));

    // V20 protocol compatibility: v3 writers can still accept a v2 packet because
    // the v2/v3 header layout is intentionally unchanged.
    let legacy = encoder.recovery_snapshot(&next)?;
    let mut legacy_bytes = legacy.encode()?;
    legacy_bytes[2] = crate::packet::MIN_WIRE_VERSION;
    checks.push(("v2 packet accepted", Packet::decode(&legacy_bytes).is_ok()));

    // Recovery snapshots must not advance the shared publisher sequence.
    let before = encoder.sequence();
    let recovery = encoder.recovery_snapshot(&next)?;
    checks.push((
        "recovery keeps publisher sequence",
        encoder.sequence() == before && recovery.sequence == before,
    ));

    // Four-way codec should select compressed snapshot for a highly compressible state.
    let policy = AdaptivePolicy::default();
    let raw = Packet::snapshot(1, 1, 1, vec![0u8; 128 * 1024]);
    let (_, decision) = policy.select_initial(raw)?;
    checks.push((
        "four-way codec can choose zstd",
        decision.mode == EncodeMode::SnapshotZstd,
    ));

    // Unknown flag bits are rejected before application.
    let mut invalid = encoder.recovery_snapshot(&next)?.encode()?;
    invalid[4] |= 0x80;
    checks.push(("unknown flags rejected", Packet::decode(&invalid).is_err()));

    let chaos = chaos::run_deterministic(2_000)?;
    checks.push(("chaos convergence", chaos.final_state_matches));
    checks.push((
        "chaos exercised recovery",
        chaos.intentionally_dropped > 0 && chaos.resyncs > 0,
    ));

    let multi = multi_client::run_deterministic(64, 1_000)?;
    checks.push(("64-client convergence", multi.all_converged()));
    checks.push((
        "multi-client exercised faults",
        multi.drops > 0 && multi.reorders > 0 && multi.disconnects > 0,
    ));

    Ok(ValidationReport { checks })
}
