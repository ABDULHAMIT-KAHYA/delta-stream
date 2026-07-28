#[test]
fn v30_fast_path_multiclient_faults_converge() {
    let report = delta_stream::v30_torture::run(128, 2_000).unwrap();
    assert!(report.all_converged());
    assert!(report.drops > 0);
    assert!(report.reorders > 0);
    assert!(report.corruptions > 0);
    assert!(report.late_joins > 0);
    assert!(report.shared_recovery_snapshots > 0);
}

#[test]
#[ignore = "heavy V30 fast-path scale test; run explicitly before release"]
fn v30_thousand_client_hard_test() {
    let report = delta_stream::v30_torture::run(1_000, 20_000).unwrap();
    assert!(report.all_converged());
}
