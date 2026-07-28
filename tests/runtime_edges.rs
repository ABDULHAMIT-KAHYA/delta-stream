use delta_stream::{
    backpressure_decision, plan_recovery, BackpressureDecision, ChangeProfile, ChunkManifest,
    FastByteStateEncoder, Packet, PartialRepair, RecoveryHistory, RuntimeLimits, SmartDeltaKind,
    StrategyAdvisor, V30RecoveryPlan,
};

#[test]
fn v30_profiles_resize_and_sparse_changes() {
    let a = vec![0u8; 4096];
    let mut b = a.clone();
    b[33] = 1;
    b[4000] = 2;
    let p = ChangeProfile::analyze(&a, &b);
    assert_eq!(p.changed_bytes, 2);
    let advisor = StrategyAdvisor::default();
    assert_eq!(advisor.shortlist(p)[0], SmartDeltaKind::Sparse);
    let c = vec![0u8; 5000];
    assert!(ChangeProfile::analyze(&a, &c).resized());
}

#[test]
fn v30_partial_repair_handles_growth_and_shrink() {
    for (a, b) in [
        (vec![1u8; 10_000], vec![1u8; 15_000]),
        (vec![2u8; 15_000], vec![2u8; 7_000]),
    ] {
        let repair = PartialRepair::build(&a, &b, 1024);
        assert_eq!(repair.apply(&a).unwrap(), b);
    }
}

#[test]
fn v30_manifest_finds_only_bad_chunks() {
    let a = vec![7u8; 8192];
    let mut b = a.clone();
    b[4096] = 9;
    let ma = ChunkManifest::build(&a, 1024);
    let mb = ChunkManifest::build(&b, 1024);
    assert_eq!(ma.differing_chunks(&mb), vec![4]);
}

#[test]
fn v30_recovery_can_choose_partial() {
    let state = vec![3u8; 128 * 1024];
    let mut receiver = state.clone();
    receiver[8192] ^= 1;
    let snapshot = Packet::snapshot(100, 0, 0, state.clone());
    let history = RecoveryHistory::new(1, 1);
    let plan = plan_recovery(&history, Some(1), Some(&receiver), &state, snapshot, 1024).unwrap();
    assert!(matches!(plan, V30RecoveryPlan::Partial(_)));
}

#[test]
fn v30_runtime_backpressure_edges() {
    let limits = RuntimeLimits {
        max_client_lag: 10,
        ..Default::default()
    };
    assert_eq!(
        backpressure_decision(None, 10, limits),
        BackpressureDecision::SnapshotAndCatchUp
    );
    assert_eq!(
        backpressure_decision(Some(10), 10, limits),
        BackpressureDecision::Accept
    );
    assert_eq!(
        backpressure_decision(Some(0), 50, limits),
        BackpressureDecision::SnapshotAndCatchUp
    );
    assert_eq!(
        backpressure_decision(Some(0), 1000, limits),
        BackpressureDecision::DropClient
    );
}

#[test]
fn v30_encoder_handles_empty_and_identical_state() {
    let mut enc = FastByteStateEncoder::new("v30/empty");
    let _ = enc.encode(&[]).unwrap();
    let _ = enc.encode(&[]).unwrap();
    assert_eq!(enc.sequence(), 2);
}
