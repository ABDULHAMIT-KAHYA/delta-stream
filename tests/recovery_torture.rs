use delta_stream::torture;

#[test]
fn deterministic_v25_torture_converges() {
    let report = torture::run(64, 2_000).unwrap();
    assert!(report.all_converged());
    assert!(report.drops > 0);
    assert!(report.reorders > 0);
    assert!(report.resyncs > 0);
    assert!(report.corruptions > 0);
    assert!(report.resync_storm_clients > 0);
    assert!(report.late_joins > 0);
}

#[test]
#[ignore = "heavy: run explicitly before release"]
fn thousand_client_hard_test() {
    let report = torture::run(1_000, 20_000).unwrap();
    assert!(report.all_converged());
}
