use delta_stream::{
    backpressure_decision, BackpressureDecision, ByteApplyResult, ByteStateDecoder,
    FastByteStateEncoder, PartialRepair, RuntimeLimits,
};

#[test]
fn v30_fast_encoder_converges_and_shortlists() {
    let mut enc = FastByteStateEncoder::new("test/v30");
    let mut dec = ByteStateDecoder::new("test/v30");
    let mut state = vec![0u8; 100 * 1024];
    for update in 0usize..500 {
        if update > 0 {
            for n in 0usize..1024 {
                let i = (update * 97 + n * 7919) % state.len();
                state[i] ^= (update as u8).wrapping_add(1);
            }
        }
        let packet = enc.encode(&state).unwrap();
        assert!(matches!(
            dec.apply(packet).unwrap(),
            ByteApplyResult::Applied { .. }
        ));
        assert!(enc.last_decision().unwrap().candidates_considered <= 5);
    }
    assert_eq!(dec.state(), Some(state.as_slice()));
}

#[test]
fn v30_partial_repair_repairs_only_changed_chunks() {
    let base = vec![1u8; 1024 * 1024];
    let mut target = base.clone();
    target[10_000..12_000].fill(9);
    let repair = PartialRepair::build(&base, &target, 1024);
    assert!(repair.payload_bytes() < target.len() / 100);
    assert_eq!(repair.apply(&base).unwrap(), target);
}

#[test]
fn v30_backpressure_is_bounded() {
    let limits = RuntimeLimits {
        max_client_lag: 100,
        ..RuntimeLimits::default()
    };
    assert_eq!(
        backpressure_decision(Some(950), 1000, limits),
        BackpressureDecision::Accept
    );
    assert_eq!(
        backpressure_decision(Some(500), 1000, limits),
        BackpressureDecision::SnapshotAndCatchUp
    );
    assert_eq!(
        backpressure_decision(Some(0), 100_000, limits),
        BackpressureDecision::DropClient
    );
}
