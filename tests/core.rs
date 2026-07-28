use delta_stream::{AgentState, ApplyResult, Decoder, Encoder, Packet};

#[test]
fn packet_roundtrip_and_crc() {
    let mut encoder = Encoder::default();
    let packet = encoder.encode(&AgentState::demo()).unwrap();
    let bytes = packet.encode().unwrap();
    assert_eq!(Packet::decode(&bytes).unwrap(), packet);
}

#[test]
fn corrupted_payload_is_rejected() {
    let mut encoder = Encoder::default();
    let packet = encoder.encode(&AgentState::demo()).unwrap();
    let mut bytes = packet.encode().unwrap();
    *bytes.last_mut().unwrap() ^= 0x55;
    assert!(Packet::decode(&bytes).is_err());
}

#[test]
fn delta_reconstructs_exact_state() {
    let a = AgentState::demo();
    let b = a.advance();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();
    let s = encoder.encode(&a).unwrap();
    let d = encoder.encode(&b).unwrap();
    let _ = decoder.apply_packet(s).unwrap();
    match decoder.apply_packet(d).unwrap() {
        ApplyResult::Applied { state, .. } => assert_eq!(state, b),
        _ => panic!("expected applied delta"),
    }
}

#[test]
fn duplicate_is_suppressed() {
    let a = AgentState::demo();
    let b = a.advance();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();
    let s = encoder.encode(&a).unwrap();
    let d = encoder.encode(&b).unwrap();
    let _ = decoder.apply_packet(s).unwrap();
    let _ = decoder.apply_packet(d.clone()).unwrap();
    assert!(matches!(
        decoder.apply_packet(d).unwrap(),
        ApplyResult::Duplicate { .. }
    ));
}

#[test]
fn missing_delta_requires_snapshot_then_recovers() {
    let a = AgentState::demo();
    let b = a.advance();
    let c = b.advance();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();
    let s = encoder.encode(&a).unwrap();
    let _d2 = encoder.encode(&b).unwrap();
    let d3 = encoder.encode(&c).unwrap();
    let _ = decoder.apply_packet(s).unwrap();
    assert!(matches!(
        decoder.apply_packet(d3).unwrap(),
        ApplyResult::NeedSnapshot { .. }
    ));
    let recovery = encoder.force_snapshot(&c).unwrap();
    match decoder.apply_packet(recovery).unwrap() {
        ApplyResult::Applied { state, .. } => assert_eq!(state, c),
        other => panic!("expected recovery snapshot, got {other:?}"),
    }
}

#[test]
fn binary_delta_is_smaller_for_demo_workload() {
    let a = AgentState::demo();
    let b = a.advance();
    let mut encoder = Encoder::default();
    let _ = encoder.encode(&a).unwrap();
    let delta = encoder.encode(&b).unwrap().encode().unwrap();
    let full = serde_json::to_vec(&b).unwrap();
    assert!(
        delta.len() < full.len(),
        "delta={} full={}",
        delta.len(),
        full.len()
    );
}

#[test]
fn adaptive_encoder_falls_back_to_snapshot_for_large_change() {
    use delta_stream::PacketKind;

    let mut a = AgentState::demo();
    let mut b = a.clone();
    a.agent_id = "A".repeat(4096);
    a.model = "B".repeat(4096);
    a.status = "C".repeat(4096);
    a.task = "D".repeat(4096);
    a.current_file = "E".repeat(4096);

    b.agent_id = "F".repeat(4096);
    b.model = "G".repeat(4096);
    b.status = "H".repeat(4096);
    b.task = "I".repeat(4096);
    b.current_file = "J".repeat(4096);

    let mut encoder = Encoder::default();
    let first = encoder.encode(&a).unwrap();
    assert_eq!(first.kind, PacketKind::Snapshot);
    let next = encoder.encode(&b).unwrap();
    assert_eq!(next.kind, PacketKind::Snapshot);
}

#[test]
fn release_validation_converges_under_chaos() {
    let report = delta_stream::chaos::run_deterministic(2_000).unwrap();
    assert!(report.intentionally_dropped > 0);
    assert!(report.resyncs > 0);
    assert!(report.final_state_matches);
}
