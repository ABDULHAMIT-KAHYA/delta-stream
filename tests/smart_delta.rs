use delta_stream::{
    smart_delta, ByteApplyResult, ByteStateDecoder, ByteStateEncoder, RecoveryHistory,
    RecoveryPlan, SmartDeltaKind,
};

#[test]
fn every_smart_delta_strategy_roundtrips() {
    let previous = (0..8192)
        .map(|i| (i as u8).wrapping_mul(31))
        .collect::<Vec<_>>();
    let mut current = previous.clone();
    for i in (0..current.len()).step_by(113) {
        current[i] ^= 0x5a;
    }
    let candidates =
        smart_delta::encode_candidates(&previous, &current, Default::default()).unwrap();
    assert!(candidates.len() >= 5);
    for candidate in candidates {
        assert_eq!(
            smart_delta::apply(&previous, &candidate.payload).unwrap(),
            current,
            "failed {:?}",
            candidate.kind
        );
    }
}

#[test]
fn splice_handles_insertions_and_deletions() {
    let a = b"abcdef012345".to_vec();
    let b = b"abcHELLOdef012345".to_vec();
    let candidates = smart_delta::encode_candidates(&a, &b, Default::default()).unwrap();
    let splice = candidates
        .into_iter()
        .find(|c| c.kind == SmartDeltaKind::Splice)
        .unwrap();
    assert_eq!(smart_delta::apply(&a, &splice.payload).unwrap(), b);
}

#[test]
fn v25_byte_engine_converges() {
    let mut enc = ByteStateEncoder::new("tests/bytes/v25");
    let mut dec = ByteStateDecoder::new("tests/bytes/v25");
    let mut state = vec![0x77; 64 * 1024];
    for update in 0..100 {
        let index = (update * 97) % state.len();
        state[index] ^= update as u8 + 1;
        let packet = enc.encode(&state).unwrap();
        match dec.apply(packet).unwrap() {
            ByteApplyResult::Applied { state: got, .. } => assert_eq!(got, state),
            other => panic!("unexpected result: {other:?}"),
        }
    }
}

#[test]
fn malformed_smart_delta_is_rejected() {
    assert!(smart_delta::apply(b"base", &[0xff, 0, 1, 2]).is_err());
}

#[test]
fn recovery_history_prefers_small_replay() {
    use delta_stream::{AgentState, Encoder};
    let mut encoder = Encoder::default();
    let mut history = RecoveryHistory::new(32, 1024 * 1024);
    let mut state = AgentState::demo();
    for _ in 0..8 {
        state = state.advance();
        history.record(encoder.encode(&state).unwrap());
    }
    let snapshot = encoder.recovery_snapshot(&state).unwrap();
    match history
        .plan(Some(encoder.sequence() - 2), snapshot)
        .unwrap()
    {
        RecoveryPlan::Replay(packets) => assert_eq!(packets.len(), 2),
        RecoveryPlan::Snapshot(_) => panic!("expected replay"),
    }
}
