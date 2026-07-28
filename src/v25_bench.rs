use std::time::{Duration, Instant};

use crate::{
    byte_sync::{ByteApplyResult, ByteEncodeMode, ByteStateDecoder, ByteStateEncoder},
    error::DeltaError,
    recovery_history::{RecoveryHistory, RecoveryPlan},
    smart_delta,
    state::AgentState,
    sync::fnv1a64,
    sync::Encoder,
    torture,
};

#[derive(Debug, Clone, Default)]
pub struct SmartBenchmarkReport {
    pub updates: usize,
    pub raw_bytes: u64,
    pub zstd_bytes: u64,
    pub v25_bytes: u64,
    pub raw_time: Duration,
    pub zstd_time: Duration,
    pub v25_encode_time: Duration,
    pub v25_decode_time: Duration,
    pub snapshots: u64,
    pub snapshot_zstd: u64,
    pub delta_sparse: u64,
    pub delta_ranges: u64,
    pub delta_xor: u64,
    pub delta_splice: u64,
    pub delta_chunks: u64,
    pub delta_zstd: u64,
}

pub fn smart_benchmark(
    state_size: usize,
    updates: usize,
    change_percent: f64,
) -> Result<SmartBenchmarkReport, DeltaError> {
    let state_size = state_size.max(1);
    let updates = updates.max(1);
    let changed =
        ((state_size as f64 * change_percent / 100.0).ceil() as usize).clamp(1, state_size);
    let mut current = high_entropy_state(state_size);
    let mut encoder = ByteStateEncoder::new("bench/high-entropy/v25");
    let mut decoder = ByteStateDecoder::new("bench/high-entropy/v25");
    let mut report = SmartBenchmarkReport {
        updates,
        ..SmartBenchmarkReport::default()
    };

    for update in 0..updates {
        mutate_scattered(&mut current, update, changed);

        let t = Instant::now();
        report.raw_bytes += current.len() as u64;
        std::hint::black_box(&current);
        report.raw_time += t.elapsed();

        let t = Instant::now();
        let compressed = zstd::bulk::compress(&current, 3)
            .map_err(|e| DeltaError::Compression(e.to_string()))?;
        report.zstd_time += t.elapsed();
        report.zstd_bytes += compressed.len() as u64;
        std::hint::black_box(compressed);

        let t = Instant::now();
        let packet = encoder.encode(&current)?;
        let wire = packet.encode()?;
        report.v25_encode_time += t.elapsed();
        report.v25_bytes += wire.len() as u64;
        count_mode(&mut report, encoder.last_decision().map(|d| d.mode));

        let t = Instant::now();
        match decoder.apply(crate::Packet::decode(&wire)?)? {
            ByteApplyResult::Applied { state, .. } if state == current => {}
            ByteApplyResult::Duplicate { .. } => {
                return Err(DeltaError::InvalidState(
                    "unexpected duplicate in V25 benchmark",
                ))
            }
            ByteApplyResult::NeedRecovery { .. } => {
                return Err(DeltaError::InvalidState(
                    "unexpected recovery in V25 benchmark",
                ))
            }
            ByteApplyResult::Applied { .. } => {
                return Err(DeltaError::InvalidState(
                    "V25 benchmark reconstruction mismatch",
                ))
            }
        }
        report.v25_decode_time += t.elapsed();
    }
    Ok(report)
}

fn count_mode(report: &mut SmartBenchmarkReport, mode: Option<ByteEncodeMode>) {
    use crate::smart_delta::SmartDeltaKind;
    match mode {
        Some(ByteEncodeMode::Snapshot) => report.snapshots += 1,
        Some(ByteEncodeMode::SnapshotZstd) => report.snapshot_zstd += 1,
        Some(ByteEncodeMode::Delta(SmartDeltaKind::Sparse)) => report.delta_sparse += 1,
        Some(ByteEncodeMode::Delta(SmartDeltaKind::Ranges)) => report.delta_ranges += 1,
        Some(ByteEncodeMode::Delta(SmartDeltaKind::Xor)) => report.delta_xor += 1,
        Some(ByteEncodeMode::Delta(SmartDeltaKind::Splice)) => report.delta_splice += 1,
        Some(ByteEncodeMode::Delta(SmartDeltaKind::Chunks)) => report.delta_chunks += 1,
        Some(ByteEncodeMode::DeltaZstd(_)) => report.delta_zstd += 1,
        None => {}
    }
}

pub fn print_smart_benchmark(
    state_size: usize,
    updates: usize,
    change_percent: f64,
) -> Result<(), DeltaError> {
    println!("=== DeltaStream V25 Smart Delta Benchmark ===");
    println!("state size       : {} bytes", state_size);
    println!("updates          : {}", updates);
    println!("change/update    : {:.3}%", change_percent);
    println!("initial data     : deterministic high entropy\n");
    let report = smart_benchmark(state_size, updates, change_percent)?;
    println!(
        "{:<24} {:>16} {:>12} {:>12}",
        "Method", "Total bytes", "Saving", "Encode s"
    );
    println!("{}", "-".repeat(70));
    print_row(
        "Raw state",
        report.raw_bytes,
        report.raw_bytes,
        report.raw_time,
    );
    print_row(
        "Raw + zstd",
        report.raw_bytes,
        report.zstd_bytes,
        report.zstd_time,
    );
    print_row(
        "DeltaStream V25",
        report.raw_bytes,
        report.v25_bytes,
        report.v25_encode_time,
    );
    println!(
        "\nV25 decode time  : {:.3} s",
        report.v25_decode_time.as_secs_f64()
    );
    println!("modes             : snap={} snap-zstd={} sparse={} ranges={} xor={} splice={} chunks={} delta-zstd={}",
        report.snapshots, report.snapshot_zstd, report.delta_sparse, report.delta_ranges,
        report.delta_xor, report.delta_splice, report.delta_chunks, report.delta_zstd);
    println!("final convergence : PASS ✓");
    Ok(())
}

pub fn print_workload_matrix() -> Result<(), DeltaError> {
    const SIZES: &[usize] = &[256, 1024, 16 * 1024, 100 * 1024];
    const CHANGES: &[f64] = &[0.1, 1.0, 5.0, 25.0, 100.0];
    const UPDATES: usize = 1_000;
    println!("=== DeltaStream V25 Workload Matrix ===");
    println!(
        "high-entropy deterministic state; {} updates/scenario\n",
        UPDATES
    );
    println!(
        "{:<10} {:>8} {:>14} {:>14} {:>10} {:>11}",
        "State", "Change", "Raw bytes", "V25 bytes", "Saving", "Mode mix"
    );
    println!("{}", "-".repeat(78));
    for &size in SIZES {
        for &change in CHANGES {
            let report = smart_benchmark(size, UPDATES, change)?;
            let saving = 100.0 * (1.0 - report.v25_bytes as f64 / report.raw_bytes as f64);
            let delta_count = report.delta_sparse
                + report.delta_ranges
                + report.delta_xor
                + report.delta_splice
                + report.delta_chunks
                + report.delta_zstd;
            println!(
                "{:<10} {:>7.1}% {:>14} {:>14} {:>9.2}% {:>5}D/{:<5}S",
                format_size(size),
                change,
                report.raw_bytes,
                report.v25_bytes,
                saving,
                delta_count,
                report.snapshots + report.snapshot_zstd
            );
        }
    }
    Ok(())
}

pub fn print_user_scale() -> Result<(), DeltaError> {
    const CLIENTS: &[usize] = &[100, 500, 1_000, 2_000];
    const UPDATES: usize = 2_000;
    println!("=== DeltaStream V25 Client Scale / Fault Matrix ===");
    println!("updates/scenario: {UPDATES}\n");
    println!(
        "{:<10} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "Clients", "Wall s", "Drops", "Reorders", "Resyncs", "Converged"
    );
    println!("{}", "-".repeat(78));
    for &clients in CLIENTS {
        let t = Instant::now();
        let report = torture::run(clients, UPDATES)?;
        let elapsed = t.elapsed();
        println!(
            "{:<10} {:>12.3} {:>12} {:>12} {:>12} {:>6}/{:<5}",
            clients,
            elapsed.as_secs_f64(),
            report.drops,
            report.reorders,
            report.resyncs,
            report.converged_clients,
            clients
        );
        if !report.all_converged() {
            return Err(DeltaError::InvalidState("client scale convergence failed"));
        }
    }
    Ok(())
}

pub fn print_recovery_matrix() -> Result<(), DeltaError> {
    const GAPS: &[u64] = &[1, 2, 5, 10, 25, 50, 100];
    let mut encoder = Encoder::default();
    let mut history = RecoveryHistory::new(256, 16 * 1024 * 1024);
    let mut state = AgentState::demo();
    for _ in 0..150 {
        state = state.advance();
        history.record(encoder.encode(&state)?);
    }
    let snapshot = encoder.recovery_snapshot(&state)?;
    println!("=== DeltaStream V25 Recovery Planner Matrix ===");
    println!(
        "current sequence: {}  snapshot bytes: {}\n",
        encoder.sequence(),
        snapshot.encoded_len()
    );
    println!("{:<10} {:>14} {:>16}", "Gap", "Strategy", "Recovery bytes");
    println!("{}", "-".repeat(44));
    for &gap in GAPS {
        let local = encoder.sequence().saturating_sub(gap);
        match history.plan(Some(local), snapshot.clone())? {
            RecoveryPlan::Replay(packets) => {
                let bytes: usize = packets.iter().map(|p| p.encoded_len()).sum();
                println!("{:<10} {:>14} {:>16}", gap, "replay", bytes);
            }
            RecoveryPlan::Snapshot(packet) => {
                println!(
                    "{:<10} {:>14} {:>16}",
                    gap,
                    "snapshot",
                    packet.encoded_len()
                );
            }
        }
    }
    Ok(())
}

pub fn print_strategy_compare() -> Result<(), DeltaError> {
    const STATE_SIZE: usize = 100 * 1024;
    const UPDATES: usize = 10_000;
    const CHANGE_PERCENT: f64 = 1.0;
    let changed = ((STATE_SIZE as f64 * CHANGE_PERCENT / 100.0).ceil() as usize).max(1);
    let schema = fnv1a64(b"bench/strategy/v25");
    let mut previous = high_entropy_state(STATE_SIZE);
    let mut current = previous.clone();
    let mut totals = std::collections::BTreeMap::<String, u64>::new();

    for update in 0..UPDATES {
        mutate_scattered(&mut current, update, changed);
        let snapshot = crate::Packet::snapshot(
            (update + 1) as u64,
            fnv1a64(&current),
            schema,
            current.clone(),
        );
        *totals.entry("Snapshot".into()).or_default() += snapshot.encoded_len() as u64;
        let sz = snapshot.zstd_candidate(3, 1)?;
        *totals.entry("Snapshot+zstd".into()).or_default() += sz.encoded_len() as u64;

        if update == 0 {
            for name in ["Sparse", "Ranges", "Xor", "Splice", "Chunks"] {
                *totals.entry(name.into()).or_default() += snapshot.encoded_len() as u64;
                *totals.entry(format!("{name}+zstd")).or_default() += sz.encoded_len() as u64;
            }
        }

        if update > 0 {
            for candidate in
                smart_delta::encode_candidates(&previous, &current, Default::default())?
            {
                let reconstructed = smart_delta::apply(&previous, &candidate.payload)?;
                if reconstructed != current {
                    return Err(DeltaError::InvalidState(
                        "strategy compare reconstruction mismatch",
                    ));
                }
                let name = format!("{:?}", candidate.kind);
                let packet = crate::Packet::delta(
                    (update + 1) as u64,
                    update as u64,
                    fnv1a64(&previous),
                    schema,
                    candidate.payload,
                );
                *totals.entry(name.clone()).or_default() += packet.encoded_len() as u64;
                let compressed = packet.zstd_candidate(3, 1)?;
                *totals.entry(format!("{name}+zstd")).or_default() +=
                    compressed.encoded_len() as u64;
            }
        }
        previous.copy_from_slice(&current);
    }

    let raw = STATE_SIZE as u64 * UPDATES as u64;
    println!("=== DeltaStream V25 Strategy Comparison ===");
    println!(
        "state={} bytes updates={} change={:.2}% high-entropy\n",
        STATE_SIZE, UPDATES, CHANGE_PERCENT
    );
    println!("{:<24} {:>16} {:>12}", "Strategy", "Total bytes", "Saving");
    println!("{}", "-".repeat(56));
    let mut rows: Vec<_> = totals.into_iter().collect();
    rows.sort_by_key(|(_, bytes)| *bytes);
    for (name, bytes) in rows {
        let saving = 100.0 * (1.0 - bytes as f64 / raw as f64);
        println!("{:<24} {:>16} {:>11.2}%", name, bytes, saving);
    }
    Ok(())
}
pub fn print_resync_storm(clients: usize) -> Result<(), DeltaError> {
    use crate::sync::{ApplyResult, Decoder};
    let clients = clients.max(1);
    let mut encoder = Encoder::default();
    let mut state = AgentState::demo();
    let first = encoder.encode(&state)?;
    let mut decoders: Vec<Decoder> = (0..clients).map(|_| Decoder::default()).collect();
    for decoder in &mut decoders {
        let _ = decoder.apply_packet(first.clone())?;
    }
    for _ in 0..500 {
        state = state.advance();
        let _ = encoder.encode(&state)?;
    }
    let t = Instant::now();
    let shared = encoder.recovery_snapshot(&state)?;
    let build_time = t.elapsed();
    let bytes_once = shared.encoded_len();
    let t = Instant::now();
    let mut converged = 0usize;
    for decoder in &mut decoders {
        match decoder.apply_packet(shared.clone())? {
            ApplyResult::Applied { state: got, .. } if got == state => converged += 1,
            ApplyResult::Duplicate { .. } if decoder.state() == Some(&state) => converged += 1,
            _ => {}
        }
    }
    let fanout_time = t.elapsed();
    println!("=== DeltaStream V25 Resync Storm ===");
    println!("clients                 : {clients}");
    println!("publisher sequence      : {}", encoder.sequence());
    println!("snapshot built          : once");
    println!("snapshot bytes          : {bytes_once}");
    println!(
        "logical cloned fanout   : {} bytes",
        bytes_once as u128 * clients as u128
    );
    println!(
        "snapshot build time     : {:.6} s",
        build_time.as_secs_f64()
    );
    println!(
        "local fanout apply time : {:.3} s",
        fanout_time.as_secs_f64()
    );
    println!("clients converged       : {converged}/{clients}");
    println!(
        "final convergence       : {}",
        if converged == clients {
            "PASS ✓"
        } else {
            "FAIL"
        }
    );
    if converged == clients {
        Ok(())
    } else {
        Err(DeltaError::InvalidState("resync storm convergence failed"))
    }
}
fn print_row(name: &str, baseline: u64, bytes: u64, elapsed: Duration) {
    let saving = if baseline == 0 {
        0.0
    } else {
        100.0 * (1.0 - bytes as f64 / baseline as f64)
    };
    println!(
        "{:<24} {:>16} {:>11.2}% {:>12.3}",
        name,
        bytes,
        saving,
        elapsed.as_secs_f64()
    );
}

fn high_entropy_state(size: usize) -> Vec<u8> {
    let mut x = 0x9e3779b97f4a7c15u64;
    let mut out = vec![0u8; size];
    for byte in &mut out {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *byte = x as u8;
    }
    out
}

fn mutate_scattered(state: &mut [u8], update: usize, changed: usize) {
    if state.is_empty() {
        return;
    }
    let stride = 2654435761usize % state.len().max(1);
    let stride = stride.max(1);
    let mut index = update.wrapping_mul(97) % state.len();
    for n in 0..changed.min(state.len()) {
        index = (index + stride + n * 17) % state.len();
        state[index] =
            state[index].wrapping_add(((update + n) as u8).wrapping_mul(31).wrapping_add(1));
    }
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{:.1}KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}
