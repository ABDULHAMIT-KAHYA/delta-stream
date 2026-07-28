use crate::{
    byte_sync::{ByteApplyResult, ByteStateDecoder, ByteStateEncoder},
    edge_cases,
    error::DeltaError,
    recovery_history::{RecoveryHistory, RecoveryPlan},
    smart_delta,
    state::AgentState,
    sync::Encoder,
    torture,
    validation::{self, ValidationReport},
};

pub fn run_release_validation() -> Result<ValidationReport, DeltaError> {
    let mut report = validation::run_release_validation()?;

    let previous = vec![0x55u8; 16 * 1024];
    let mut current = previous.clone();
    for i in (0..current.len()).step_by(257) {
        current[i] ^= 0x7f;
    }
    let candidates = smart_delta::encode_candidates(&previous, &current, Default::default())?;
    let exact = candidates.iter().all(|candidate| {
        smart_delta::apply(&previous, &candidate.payload)
            .ok()
            .as_deref()
            == Some(current.as_slice())
    });
    report
        .checks
        .push(("V25 smart delta strategies exact", exact));

    let mut encoder = ByteStateEncoder::new("validation/bytes/v25");
    let mut decoder = ByteStateDecoder::new("validation/bytes/v25");
    let p1 = encoder.encode(&previous)?;
    let p2 = encoder.encode(&current)?;
    let _ = decoder.apply(p1)?;
    report.checks.push((
        "V25 adaptive byte engine applies",
        matches!(decoder.apply(p2)?, ByteApplyResult::Applied { state, .. } if state == current),
    ));

    let edges = edge_cases::run(false)?;
    report
        .checks
        .push(("V25 edge-case suite", edges.all_passed()));

    let mut history = RecoveryHistory::new(32, 1024 * 1024);
    let mut state = AgentState::demo();
    let mut agent_encoder = Encoder::default();
    for _ in 0..6 {
        state = state.advance();
        history.record(agent_encoder.encode(&state)?);
    }
    let snapshot = agent_encoder.recovery_snapshot(&state)?;
    report.checks.push((
        "V25 recovery planner replays small gap",
        matches!(history.plan(Some(4), snapshot)?, RecoveryPlan::Replay(_)),
    ));

    let hard = torture::run(128, 2_000)?;
    report
        .checks
        .push(("V25 torture convergence", hard.all_converged()));
    report
        .checks
        .push(("V25 torture exercised late joins", hard.late_joins > 0));
    report.checks.push((
        "V25 torture exercised resync storm",
        hard.resync_storm_clients > 0,
    ));
    report.checks.push((
        "V25 shared storm snapshot",
        hard.resync_storm_clients > 1 && hard.resync_storm_snapshots_built == 1,
    ));

    Ok(report)
}
