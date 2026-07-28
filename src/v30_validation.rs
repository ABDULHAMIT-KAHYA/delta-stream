use crate::{
    byte_sync::{ByteApplyResult, ByteStateDecoder},
    error::DeltaError,
    partial_repair::PartialRepair,
    runtime::{backpressure_decision, BackpressureDecision, RuntimeLimits},
    smart_delta::SmartDeltaKind,
    v30_sync::{FastByteStateEncoder, V30EncodeMode},
};

#[derive(Debug, Default)]
pub struct V30ValidationReport {
    pub checks: Vec<(&'static str, bool)>,
}
impl V30ValidationReport {
    pub fn all_passed(&self) -> bool {
        self.checks.iter().all(|(_, ok)| *ok)
    }
}

pub fn run() -> Result<V30ValidationReport, DeltaError> {
    let mut r = V30ValidationReport::default();

    let mut enc = FastByteStateEncoder::new("v30/validate");
    let mut dec = ByteStateDecoder::new("v30/validate");
    let mut state = vec![0xA5u8; 16 * 1024];
    let p1 = enc.encode(&state)?;
    r.checks.push((
        "V30 initial snapshot",
        matches!(dec.apply(p1)?, ByteApplyResult::Applied { .. }),
    ));
    state[777] ^= 0x5A;
    let p2 = enc.encode(&state)?;
    r.checks.push((
        "V30 fast delta applies",
        matches!(dec.apply(p2)?, ByteApplyResult::Applied { .. }),
    ));
    r.checks.push((
        "V30 selector bounded candidates",
        enc.last_decision()
            .is_some_and(|d| d.candidates_considered <= 5),
    ));
    r.checks.push((
        "V30 sparse workload classified",
        enc.last_decision().is_some_and(|d| {
            matches!(
                d.mode,
                V30EncodeMode::Delta(SmartDeltaKind::Sparse)
                    | V30EncodeMode::DeltaZstd(SmartDeltaKind::Sparse)
                    | V30EncodeMode::Delta(SmartDeltaKind::Ranges)
                    | V30EncodeMode::DeltaZstd(SmartDeltaKind::Ranges)
            )
        }),
    ));

    let mut receiver = vec![0x11u8; 256 * 1024];
    let mut authoritative = receiver.clone();
    authoritative[1024..2048].fill(0x22);
    authoritative[200_000..201_024].fill(0x33);
    let repair = PartialRepair::build(&receiver, &authoritative, 1024);
    receiver = repair.apply(&receiver)?;
    r.checks
        .push(("V30 partial repair exact", receiver == authoritative));
    r.checks.push((
        "V30 partial repair smaller than snapshot",
        repair.payload_bytes() < authoritative.len(),
    ));

    let limits = RuntimeLimits {
        max_client_lag: 100,
        ..RuntimeLimits::default()
    };
    r.checks.push((
        "V30 backpressure accepts healthy client",
        backpressure_decision(Some(950), 1_000, limits) == BackpressureDecision::Accept,
    ));
    r.checks.push((
        "V30 backpressure snapshots slow client",
        backpressure_decision(Some(500), 1_000, limits) == BackpressureDecision::SnapshotAndCatchUp,
    ));
    r.checks.push((
        "V30 backpressure drops pathological lag",
        backpressure_decision(Some(0), 100_000, limits) == BackpressureDecision::DropClient,
    ));

    let fast_hard = crate::v30_torture::run(128, 2_000)?;
    r.checks.push((
        "V30 fast-path multiclient convergence",
        fast_hard.all_converged(),
    ));
    r.checks.push((
        "V30 fast-path faults exercised",
        fast_hard.drops > 0 && fast_hard.reorders > 0 && fast_hard.corruptions > 0,
    ));

    let old = crate::v25_validation::run_release_validation()?;
    r.checks
        .push(("V25 regression suite preserved", old.all_passed()));
    Ok(r)
}
