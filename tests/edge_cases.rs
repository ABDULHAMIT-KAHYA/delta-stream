use delta_stream::{
    edge_cases,
    reorder::{ReorderApplyResult, ReorderDecoder},
    AgentState, Encoder,
};

#[test]
fn v25_edge_suite_light_passes() {
    let report = edge_cases::run(false).unwrap();
    for (name, ok) in &report.checks {
        assert!(*ok, "edge case failed: {name}");
    }
}

#[test]
fn reorder_window_avoids_unnecessary_resync() {
    let mut encoder = Encoder::default();
    let a = AgentState::demo();
    let b = a.advance();
    let c = b.advance();
    let p1 = encoder.encode(&a).unwrap();
    let p2 = encoder.encode(&b).unwrap();
    let p3 = encoder.encode(&c).unwrap();
    let mut decoder = ReorderDecoder::new(4, 8);
    assert!(matches!(
        decoder.apply(p1).unwrap(),
        ReorderApplyResult::Applied { .. }
    ));
    assert!(matches!(
        decoder.apply(p3).unwrap(),
        ReorderApplyResult::Buffered { .. }
    ));
    assert!(matches!(
        decoder.apply(p2).unwrap(),
        ReorderApplyResult::Applied { drained: 1, .. }
    ));
    assert_eq!(decoder.state(), Some(&c));
}
