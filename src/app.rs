use crate::{
    chaos, demo_server,
    error::DeltaError,
    packet::PacketKind,
    state::AgentState,
    sync::{ApplyResult, Decoder, Encoder},
    validation,
};

pub fn run() -> Result<(), DeltaError> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("--serve") => {
            demo_server::serve(args.get(1).map(String::as_str).unwrap_or("127.0.0.1:8787"))
        }
        Some("--benchmark") => benchmark_demo(),
        Some("--scenario-benchmark") => scenario_benchmark(),
        Some("--generic-demo") => generic_demo(),
        Some("--validate-v30") => validate_v30(),
        Some("--validate-v25") => validate_v25(),
        Some("--validate-v20") | Some("--validate-v15") => validate_v20(),
        Some("--compare-benchmark") => comparison_benchmark(),
        Some("--chaos") => chaos_demo(10_000),
        Some("--soak") => chaos_demo(100_000),
        Some("--capabilities") => capabilities_demo(),
        Some("--scale-benchmark") => scale_benchmark(),
        Some("--large-compare") => large_comparison_benchmark(),
        Some("--multi-client") => multi_client_demo(100, 10_000),
        Some("--torture") => torture_demo(1_000, 20_000),
        Some("--torture-max") => {
            let clients = args
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(2_000);
            let updates = args
                .get(2)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(50_000);
            torture_demo(clients, updates)
        }
        Some("--edge-cases") => edge_case_demo(true),
        Some("--v30-benchmark") => {
            let updates = args
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(100_000);
            crate::v30_bench::print_fast_encoder_benchmark(100 * 1024, updates, 1.0)
        }
        Some("--v30-torture") => v30_torture_demo(1_000, 20_000),
        Some("--v30-torture-max") => {
            let clients = args
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(2_000);
            let updates = args
                .get(2)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(50_000);
            v30_torture_demo(clients, updates)
        }
        Some("--partial-repair") => crate::v30_bench::print_partial_repair_demo(),
        Some("--smart-delta-benchmark") => {
            crate::v25_bench::print_smart_benchmark(100 * 1024, 100_000, 1.0)
        }
        Some("--strategy-compare") => crate::v25_bench::print_strategy_compare(),
        Some("--workload-matrix") => crate::v25_bench::print_workload_matrix(),
        Some("--user-scale") => crate::v25_bench::print_user_scale(),
        Some("--recovery-matrix") => crate::v25_bench::print_recovery_matrix(),
        Some("--resync-storm") => {
            let clients = args
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(10_000);
            crate::v25_bench::print_resync_storm(clients)
        }
        Some("--help") | Some("-h") => {
            print_help();
            Ok(())
        }

        #[cfg(feature = "pubnub-transport")]
        Some("--pubnub-publish") => pubnub_publish_demo(),
        #[cfg(feature = "pubnub-transport")]
        Some("--pubnub-listen") => pubnub_listen_demo(),
        #[cfg(feature = "websocket-transport")]
        Some("--ws-broker") => {
            websocket_broker_demo(args.get(1).map(String::as_str).unwrap_or("127.0.0.1:8790"))
        }
        #[cfg(not(feature = "pubnub-transport"))]
        Some("--pubnub-publish") | Some("--pubnub-listen") => {
            println!("PubNub transport is optional. Run with --features pubnub-transport");
            Ok(())
        }
        Some(other) => {
            eprintln!("unknown option: {other}\n");
            print_help();
            Ok(())
        }
        None => local_demo(),
    }
}

fn local_demo() -> Result<(), DeltaError> {
    let initial = AgentState::demo();
    let second = initial.advance();
    let third = second.advance();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();

    let snapshot = encoder.encode(&initial)?;
    let delta_2 = encoder.encode(&second)?;
    let delta_3 = encoder.encode(&third)?;
    let full_json = serde_json::to_vec(&second)?.len();

    println!("=== DeltaStream V30 ===");
    println!("library version    : {}", env!("CARGO_PKG_VERSION"));
    println!("wire protocol      : v{}", crate::packet::WIRE_VERSION);
    println!("full JSON state    : {full_json} bytes");
    println!(
        "initial packet     : {:?} / {} bytes",
        snapshot.kind,
        snapshot.encoded_len()
    );
    println!(
        "second packet      : {:?} / {} bytes",
        delta_2.kind,
        delta_2.encoded_len()
    );
    if let Some(d) = encoder.last_decision() {
        println!(
            "adaptive decision  : {:?} (snapshot={} delta={:?} selected={})",
            d.mode, d.snapshot_bytes, d.delta_bytes, d.selected_bytes
        );
    }

    match decoder.apply_packet(snapshot)? {
        ApplyResult::Applied { sequence, state } => {
            println!(
                "snapshot applied   : seq={sequence} progress={}",
                state.progress
            );
        }
        _ => unreachable!(),
    }

    println!("dropping update #2 to validate recovery...");
    match decoder.apply_packet(delta_3)? {
        ApplyResult::NeedSnapshot {
            local_sequence,
            required_sequence,
        } => {
            println!("DESYNC detected    : local={local_sequence:?} requires={required_sequence}");
            let recovery = encoder.recovery_snapshot(&third)?;
            match decoder.apply_packet(recovery)? {
                ApplyResult::Applied { sequence, state } => {
                    println!(
                        "SYNC RESTORED      : seq={sequence} progress={} ✓",
                        state.progress
                    );
                }
                _ => return Err(DeltaError::InvalidState("local resync failed")),
            }
        }
        ApplyResult::Applied { .. } => {
            // An adaptive snapshot is self-contained and may safely heal the chain itself.
            println!("adaptive snapshot applied safely without explicit resync ✓");
        }
        ApplyResult::Duplicate { .. } => {}
    }

    println!(
        "Capabilities       : {:?}",
        crate::compat::Capabilities::local()
    );
    println!("\nRelease validation: cargo run --release -- --validate-v30");
    println!("Chaos test         : cargo run --release -- --chaos");
    println!("Soak test          : cargo run --release -- --soak");
    Ok(())
}

fn v30_torture_demo(clients: usize, updates: usize) -> Result<(), DeltaError> {
    let started = std::time::Instant::now();
    let report = crate::v30_torture::run(clients, updates)?;
    println!("=== DeltaStream V30 Fast-Path Torture ===");
    println!("clients             : {}", report.clients);
    println!("updates             : {}", report.updates);
    println!("drops               : {}", report.drops);
    println!("duplicates          : {}", report.duplicates);
    println!("corruptions         : {}", report.corruptions);
    println!("reorders            : {}", report.reorders);
    println!("recoveries          : {}", report.recoveries);
    println!("shared snapshots    : {}", report.shared_recovery_snapshots);
    println!("late joins          : {}", report.late_joins);
    println!(
        "clients converged   : {}/{}",
        report.converged, report.clients
    );
    println!(
        "wall time           : {:.3} s",
        started.elapsed().as_secs_f64()
    );
    println!(
        "final convergence   : {}",
        if report.all_converged() {
            "PASS ✓"
        } else {
            "FAIL"
        }
    );
    if report.all_converged() {
        Ok(())
    } else {
        Err(DeltaError::InvalidState("V30 fast-path torture failed"))
    }
}

fn validate_v30() -> Result<(), DeltaError> {
    let report = crate::v30_validation::run()?;
    println!("=== DeltaStream V30 Release Validation ===");
    for (name, ok) in &report.checks {
        println!("{name:<44} {}", if *ok { "PASS" } else { "FAIL" });
    }
    if report.all_passed() {
        println!("\nV30 validation: PASS ✓");
        Ok(())
    } else {
        Err(DeltaError::InvalidState("V30 validation failed"))
    }
}

fn generic_demo() -> Result<(), DeltaError> {
    use crate::{GenericApplyResult, GenericDecoder, GenericEncoder};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "derive", derive(crate::DeltaState))]
    struct GameState {
        x: i32,
        y: i32,
        hp: u16,
        weapon: String,
    }

    #[cfg(not(feature = "derive"))]
    impl crate::schema::DeltaState for GameState {
        const SCHEMA_NAME: &'static str = "GameState";
    }

    let a = GameState {
        x: 10,
        y: 20,
        hp: 100,
        weapon: "rifle".into(),
    };
    let b = GameState {
        x: 11,
        y: 20,
        hp: 98,
        weapon: "rifle".into(),
    };
    let mut encoder = GenericEncoder::<GameState>::default();
    let mut decoder = GenericDecoder::<GameState>::default();
    let p1 = encoder.encode(&a)?;
    let p2 = encoder.encode(&b)?;
    let _ = decoder.apply_packet(p1)?;
    match decoder.apply_packet(p2)? {
        GenericApplyResult::Applied { sequence, state } => {
            println!("V25 generic adaptive sync seq={sequence}: {state:?}");
        }
        _ => return Err(DeltaError::InvalidState("generic demo failed")),
    }
    Ok(())
}

fn scale_benchmark() -> Result<(), DeltaError> {
    use std::time::Instant;

    const UPDATES: usize = 100_000_000;

    println!("=== DeltaStream V25 Scale Benchmark ===");
    println!("Updates: {UPDATES}");

    let mut state = AgentState::demo();
    let mut encoder = Encoder::default();

    let mut full_json_bytes = 0usize;
    let mut delta_stream_bytes = 0usize;

    let started = Instant::now();

    for _ in 0..UPDATES {
        state = state.advance();

        // Raw JSON baseline
        let json = serde_json::to_vec(&state)?;
        full_json_bytes += json.len();

        // DeltaStream
        let packet = encoder.encode(&state)?;
        let wire = packet.encode()?;
        delta_stream_bytes += wire.len();
    }

    let elapsed = started.elapsed();

    let saving = (1.0 - delta_stream_bytes as f64 / full_json_bytes as f64) * 100.0;

    let updates_per_sec = UPDATES as f64 / elapsed.as_secs_f64();

    println!();
    println!("Logical JSON traffic : {} bytes", full_json_bytes);
    println!("DeltaStream traffic  : {} bytes", delta_stream_bytes);
    println!("Reduction            : {:.2}%", saving);
    println!("Elapsed              : {:.3} s", elapsed.as_secs_f64());
    println!("Throughput           : {:.0} updates/sec", updates_per_sec);

    Ok(())
}

fn comparison_benchmark() -> Result<(), DeltaError> {
    use std::time::Instant;

    const UPDATES: usize = 10_000;
    const ZSTD_LEVEL: i32 = 3;

    println!("=== DeltaStream V25 Competitive Benchmark ===");
    println!("Updates    : {UPDATES}");
    println!("Zstd level : {ZSTD_LEVEL}");
    println!();

    // ------------------------------------------------------------
    // Prepare identical states once.
    // Every codec gets the exact same workload.
    // ------------------------------------------------------------
    let mut states = Vec::with_capacity(UPDATES);

    let mut state = AgentState::demo();

    for _ in 0..UPDATES {
        state = state.advance();
        states.push(state.clone());
    }

    // ============================================================
    // 1. RAW JSON
    // ============================================================
    let json_encode_started = Instant::now();

    let mut raw_json_messages = Vec::with_capacity(UPDATES);
    let mut raw_json_total = 0usize;

    for state in &states {
        let bytes = serde_json::to_vec(state)?;
        raw_json_total += bytes.len();
        raw_json_messages.push(bytes);
    }

    let json_encode_elapsed = json_encode_started.elapsed();

    let json_decode_started = Instant::now();

    for bytes in &raw_json_messages {
        let decoded: AgentState = serde_json::from_slice(bytes)?;
        std::hint::black_box(decoded);
    }

    let json_decode_elapsed = json_decode_started.elapsed();

    // ============================================================
    // 2. JSON + ZSTD
    //
    // IMPORTANT:
    // Each update is compressed independently.
    // This represents per-message compression, not a continuous
    // compression stream.
    // ============================================================
    let zstd_encode_started = Instant::now();

    let mut zstd_messages = Vec::with_capacity(UPDATES);
    let mut zstd_total = 0usize;

    for state in &states {
        let json = serde_json::to_vec(state)?;

        let compressed =
            zstd::stream::encode_all(json.as_slice(), ZSTD_LEVEL).map_err(DeltaError::Io)?;

        zstd_total += compressed.len();
        zstd_messages.push(compressed);
    }

    let zstd_encode_elapsed = zstd_encode_started.elapsed();

    let zstd_decode_started = Instant::now();

    for compressed in &zstd_messages {
        let json = zstd::stream::decode_all(compressed.as_slice()).map_err(DeltaError::Io)?;

        let decoded: AgentState = serde_json::from_slice(&json)?;

        std::hint::black_box(decoded);
    }

    let zstd_decode_elapsed = zstd_decode_started.elapsed();

    // ============================================================
    // 3. JSON PATCH
    //
    // First message = complete JSON state.
    // Following messages = RFC 6902 patches from previous state.
    // ============================================================
    let patch_encode_started = Instant::now();

    let mut patch_messages: Vec<Vec<u8>> = Vec::with_capacity(UPDATES);

    let mut patch_total = 0usize;

    let first_value = serde_json::to_value(&states[0])?;

    let first_json = serde_json::to_vec(&first_value)?;

    patch_total += first_json.len();
    patch_messages.push(first_json);

    let mut previous_value = first_value;

    for state in states.iter().skip(1) {
        let current_value = serde_json::to_value(state)?;

        let patch = json_patch::diff(&previous_value, &current_value);

        let bytes = serde_json::to_vec(&patch)?;

        patch_total += bytes.len();
        patch_messages.push(bytes);

        previous_value = current_value;
    }

    let patch_encode_elapsed = patch_encode_started.elapsed();

    // Decode/apply JSON Patch chain.
    let patch_decode_started = Instant::now();

    let mut reconstructed: serde_json::Value = serde_json::from_slice(&patch_messages[0])?;

    for bytes in patch_messages.iter().skip(1) {
        let patch: json_patch::Patch = serde_json::from_slice(bytes)?;

        json_patch::patch(&mut reconstructed, &patch)
            .map_err(|_| DeltaError::InvalidState("JSON Patch apply failed"))?;
    }

    let final_patch_state: AgentState = serde_json::from_value(reconstructed)?;

    std::hint::black_box(final_patch_state);

    let patch_decode_elapsed = patch_decode_started.elapsed();

    // ============================================================
    // 4. DELTASTREAM
    //
    // Includes:
    // Encoder::encode
    // Packet::encode
    //
    // Decode benchmark includes:
    // Packet::decode
    // Decoder::apply_packet
    // ============================================================
    let ds_encode_started = Instant::now();

    let mut ds_encoder = Encoder::default();

    let mut ds_messages = Vec::with_capacity(UPDATES);

    let mut ds_total = 0usize;

    let mut delta_count = 0usize;
    let mut snapshot_count = 0usize;

    for state in &states {
        let packet = ds_encoder.encode(state)?;

        match packet.kind {
            crate::packet::PacketKind::Delta => {
                delta_count += 1;
            }

            crate::packet::PacketKind::Snapshot => {
                snapshot_count += 1;
            }
        }

        let bytes = packet.encode()?;

        ds_total += bytes.len();
        ds_messages.push(bytes);
    }

    let ds_encode_elapsed = ds_encode_started.elapsed();

    let ds_decode_started = Instant::now();

    let mut ds_decoder = Decoder::default();

    for bytes in &ds_messages {
        let packet = crate::Packet::decode(bytes)?;

        let result = ds_decoder.apply_packet(packet)?;

        std::hint::black_box(result);
    }

    let ds_decode_elapsed = ds_decode_started.elapsed();

    // ============================================================
    // RESULTS
    // ============================================================

    println!(
        "{:<18} {:>14} {:>12} {:>14} {:>14}",
        "Method", "Total bytes", "Bytes/msg", "Encode ms", "Decode ms"
    );

    println!("{}", "-".repeat(78));

    print_compare_row(
        "Raw JSON",
        raw_json_total,
        UPDATES,
        json_encode_elapsed,
        json_decode_elapsed,
    );

    print_compare_row(
        "JSON + zstd",
        zstd_total,
        UPDATES,
        zstd_encode_elapsed,
        zstd_decode_elapsed,
    );

    print_compare_row(
        "JSON Patch",
        patch_total,
        UPDATES,
        patch_encode_elapsed,
        patch_decode_elapsed,
    );

    print_compare_row(
        "DeltaStream",
        ds_total,
        UPDATES,
        ds_encode_elapsed,
        ds_decode_elapsed,
    );

    println!();

    println!(
        "DeltaStream packets: {delta_count} deltas / \
         {snapshot_count} snapshots"
    );

    println!();

    println!("Savings vs Raw JSON:");

    print_saving("JSON + zstd", raw_json_total, zstd_total);

    print_saving("JSON Patch", raw_json_total, patch_total);

    print_saving("DeltaStream", raw_json_total, ds_total);

    Ok(())
}

fn print_compare_row(
    name: &str,
    total_bytes: usize,
    updates: usize,
    encode_time: std::time::Duration,
    decode_time: std::time::Duration,
) {
    println!(
        "{:<18} {:>14} {:>12.2} {:>14.3} {:>14.3}",
        name,
        total_bytes,
        total_bytes as f64 / updates as f64,
        encode_time.as_secs_f64() * 1000.0,
        decode_time.as_secs_f64() * 1000.0,
    );
}

fn print_saving(name: &str, baseline: usize, value: usize) {
    let reduction = (1.0 - value as f64 / baseline as f64) * 100.0;

    println!("  {:<16}: {:>7.2}%", name, reduction);
}

fn benchmark_demo() -> Result<(), DeltaError> {
    let mut state = AgentState::demo();
    let mut encoder = Encoder::default();
    let mut full_total = 0usize;
    let mut wire_total = 0usize;
    let mut deltas = 0usize;
    let mut snapshots = 0usize;
    let updates = 10_000usize;

    for _ in 0..updates {
        state = state.advance();
        full_total += serde_json::to_vec(&state)?.len();
        let packet = encoder.encode(&state)?;
        wire_total += packet.encode()?.len();
        match packet.kind {
            PacketKind::Delta => deltas += 1,
            PacketKind::Snapshot => snapshots += 1,
        }
    }

    println!("=== DeltaStream V25 10,000-update adaptive benchmark ===");
    println!("Full JSON   : {full_total} bytes");
    println!("DeltaStream : {wire_total} bytes");
    println!(
        "Reduction   : {:.2}%",
        (1.0 - wire_total as f64 / full_total as f64) * 100.0
    );
    println!("Deltas      : {deltas}");
    println!("Snapshots   : {snapshots}");
    Ok(())
}

fn scenario_benchmark() -> Result<(), DeltaError> {
    const STATE_SIZES: &[usize] = &[250, 500, 1024, 1800, 2048, 4096, 8192, 16384, 30000];
    const CHANGE_RATES: &[f64] = &[0.01, 0.05, 0.10, 0.25, 0.50, 1.00];
    const UPDATES: usize = 10_000;

    println!("=== DeltaStream V25 Sparse-State Scenario Benchmark ===");
    println!("Updates per scenario: {UPDATES}");
    println!("Adaptive rule: sparse delta if smaller, otherwise snapshot\n");
    println!(
        "{:<10} {:<8} {:>13} {:>13} {:>9} {:>9} {:>9}",
        "State", "Change", "Full bytes", "Wire bytes", "Saving", "Deltas", "Snaps"
    );
    println!("{}", "-".repeat(86));

    for &state_size in STATE_SIZES {
        for &change_rate in CHANGE_RATES {
            run_sparse_scenario(state_size, change_rate, UPDATES)?;
        }
    }
    Ok(())
}

fn run_sparse_scenario(
    state_size: usize,
    change_rate: f64,
    updates: usize,
) -> Result<(), DeltaError> {
    use crate::{packet::Packet, sync::fnv1a64};

    let changed_bytes = ((state_size as f64 * change_rate).ceil() as usize).clamp(1, state_size);
    let mut previous = vec![0u8; state_size];
    let mut current = previous.clone();
    let schema_hash = fnv1a64(b"delta-stream/SparseByteState/v20");
    let mut full_total = 0usize;
    let mut wire_total = 0usize;
    let mut deltas = 0usize;
    let mut snapshots = 0usize;

    for update in 0..updates {
        for offset in 0..changed_bytes {
            let index = (update + offset) % state_size;
            current[index] = current[index].wrapping_add(1);
        }
        full_total += current.len();
        let sequence = update as u64 + 1;
        let snapshot = Packet::snapshot(sequence, fnv1a64(&current), schema_hash, current.clone());

        let selected = if sequence == 1 {
            snapshot
        } else {
            let payload = encode_sparse_byte_delta(&previous, &current);
            let delta = Packet::delta(
                sequence,
                sequence - 1,
                fnv1a64(&previous),
                schema_hash,
                payload,
            );
            if delta.encoded_len() < snapshot.encoded_len() {
                delta
            } else {
                snapshot
            }
        };
        match selected.kind {
            PacketKind::Delta => deltas += 1,
            PacketKind::Snapshot => snapshots += 1,
        }
        wire_total += selected.encode()?.len();
        previous.copy_from_slice(&current);
    }

    let saving = (1.0 - wire_total as f64 / full_total as f64) * 100.0;
    println!(
        "{:<10} {:>6.0}% {:>13} {:>13} {:>8.2}% {:>9} {:>9}",
        format_size(state_size),
        change_rate * 100.0,
        full_total,
        wire_total,
        saving,
        deltas,
        snapshots
    );
    Ok(())
}

fn encode_sparse_byte_delta(previous: &[u8], current: &[u8]) -> Vec<u8> {
    let mut changes = Vec::new();
    for (index, (&old, &new)) in previous.iter().zip(current.iter()).enumerate() {
        if old != new {
            changes.push((index, new));
        }
    }
    let mut out = Vec::with_capacity(8 + changes.len() * 3);
    put_var_u64(&mut out, changes.len() as u64);
    let mut previous_index = 0usize;
    for (n, (index, value)) in changes.into_iter().enumerate() {
        let index_delta = if n == 0 {
            index
        } else {
            index - previous_index
        };
        put_var_u64(&mut out, index_delta as u64);
        out.push(value);
        previous_index = index;
    }
    out
}

fn put_var_u64(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn multi_client_demo(clients: usize, updates: usize) -> Result<(), DeltaError> {
    let report = crate::multi_client::run_deterministic(clients, updates)?;
    println!("=== DeltaStream V25 Multi-Client Baseline Convergence ===");
    println!("clients             : {}", report.clients);
    println!("updates             : {}", report.updates);
    println!("deliveries          : {}", report.deliveries);
    println!("drops               : {}", report.drops);
    println!("duplicates          : {}", report.duplicates);
    println!("reorders            : {}", report.reorders);
    println!("disconnects         : {}", report.disconnects);
    println!("resyncs             : {}", report.resyncs);
    println!("final sequence      : {}", report.final_sequence);
    println!(
        "clients converged   : {}/{}",
        report.converged_clients, report.clients
    );
    println!(
        "final convergence   : {}",
        if report.all_converged() {
            "PASS ✓"
        } else {
            "FAIL"
        }
    );
    if report.all_converged() {
        Ok(())
    } else {
        Err(DeltaError::InvalidState("multi-client convergence failed"))
    }
}

fn large_comparison_benchmark() -> Result<(), DeltaError> {
    use crate::{packet::Packet, sync::fnv1a64};
    use std::time::{Duration, Instant};

    const STATE_SIZE: usize = 100 * 1024;
    const UPDATES: usize = 100_000;
    const CHANGE_RATE: f64 = 0.01;
    const ZSTD_LEVEL: i32 = 3;

    let changed_bytes = ((STATE_SIZE as f64 * CHANGE_RATE).ceil() as usize).clamp(1, STATE_SIZE);
    let schema_hash = fnv1a64(b"delta-stream/HighEntropySparseByteState/v20");

    println!("=== DeltaStream V25 Large Competitive Benchmark ===");
    println!("State size     : {} bytes", STATE_SIZE);
    println!("Updates        : {}", UPDATES);
    println!(
        "Logical traffic: {:.2} GB",
        STATE_SIZE as f64 * UPDATES as f64 / 1_000_000_000.0
    );
    println!("Change rate    : {:.2}%", CHANGE_RATE * 100.0);
    println!("Changed/update : {} bytes", changed_bytes);
    println!("Initial data   : deterministic high-entropy bytes");
    println!("Zstd level     : {}", ZSTD_LEVEL);
    println!();

    let mut current = pseudo_random_state(STATE_SIZE);
    let mut previous = current.clone();
    let mut reconstructed_ds = vec![0u8; STATE_SIZE];
    let mut reconstructed_dsz = vec![0u8; STATE_SIZE];

    let mut raw_total = 0u64;
    let mut zstd_total = 0u64;
    let mut ds_total = 0u64;
    let mut dsz_total = 0u64;

    let mut raw_encode = Duration::ZERO;
    let mut raw_decode = Duration::ZERO;
    let mut zstd_encode = Duration::ZERO;
    let mut zstd_decode = Duration::ZERO;
    let mut ds_encode = Duration::ZERO;
    let mut ds_decode = Duration::ZERO;
    let mut dsz_encode = Duration::ZERO;
    let mut dsz_decode = Duration::ZERO;

    let wall = Instant::now();

    for update in 0..UPDATES {
        for offset in 0..changed_bytes {
            let index = (update
                .wrapping_mul(104729)
                .wrapping_add(offset.wrapping_mul(97)))
                % STATE_SIZE;
            current[index] = current[index]
                .wrapping_add(((update + offset) as u8).wrapping_mul(31).wrapping_add(1));
        }
        let sequence = update as u64 + 1;

        // Raw bytes: copy into a transport-owned message and copy back on decode.
        let t = Instant::now();
        let raw_message = current.clone();
        raw_encode += t.elapsed();
        raw_total += raw_message.len() as u64;
        let t = Instant::now();
        let raw_decoded = raw_message.clone();
        raw_decode += t.elapsed();
        std::hint::black_box(raw_decoded);

        // Full state + zstd.
        let t = Instant::now();
        let compressed =
            zstd::stream::encode_all(current.as_slice(), ZSTD_LEVEL).map_err(DeltaError::Io)?;
        zstd_encode += t.elapsed();
        zstd_total += compressed.len() as u64;
        let t = Instant::now();
        let uncompressed =
            zstd::stream::decode_all(compressed.as_slice()).map_err(DeltaError::Io)?;
        zstd_decode += t.elapsed();
        if uncompressed != current {
            return Err(DeltaError::InvalidState("zstd roundtrip failed"));
        }

        // Raw DeltaStream sparse packet.
        let t = Instant::now();
        let raw_packet = if sequence == 1 {
            Packet::snapshot(sequence, fnv1a64(&current), schema_hash, current.clone())
        } else {
            Packet::delta(
                sequence,
                sequence - 1,
                fnv1a64(&previous),
                schema_hash,
                encode_sparse_byte_delta(&previous, &current),
            )
        };
        let ds_wire = raw_packet.encode()?;
        ds_encode += t.elapsed();
        ds_total += ds_wire.len() as u64;
        let t = Instant::now();
        apply_sparse_packet(&mut reconstructed_ds, Packet::decode(&ds_wire)?)?;
        ds_decode += t.elapsed();

        // End-to-end DeltaStream + zstd: delta construction + payload compression + wire encoding.
        let t = Instant::now();
        let dsz_raw = if sequence == 1 {
            Packet::snapshot(sequence, fnv1a64(&current), schema_hash, current.clone())
        } else {
            Packet::delta(
                sequence,
                sequence - 1,
                fnv1a64(&previous),
                schema_hash,
                encode_sparse_byte_delta(&previous, &current),
            )
        };
        let dsz_packet = dsz_raw.zstd_candidate(ZSTD_LEVEL, 1)?;
        let dsz_wire = dsz_packet.encode()?;
        dsz_encode += t.elapsed();
        dsz_total += dsz_wire.len() as u64;
        let t = Instant::now();
        apply_sparse_packet(&mut reconstructed_dsz, Packet::decode(&dsz_wire)?)?;
        dsz_decode += t.elapsed();

        previous.copy_from_slice(&current);

        if sequence.is_multiple_of(10_000) {
            println!(
                "progress: {:>6} / {} ({:>5.1}%)",
                sequence,
                UPDATES,
                sequence as f64 / UPDATES as f64 * 100.0
            );
        }
    }

    if reconstructed_ds != current || reconstructed_dsz != current {
        return Err(DeltaError::InvalidState(
            "DeltaStream large benchmark did not converge",
        ));
    }

    println!("\n=== RESULTS ===\n");
    println!(
        "{:<22} {:>15} {:>10} {:>12} {:>12}",
        "Method", "Total bytes", "Saving", "Encode s", "Decode s"
    );
    println!("{}", "-".repeat(76));
    print_large_row("Raw state", raw_total, raw_total, raw_encode, raw_decode);
    print_large_row(
        "Raw + zstd",
        raw_total,
        zstd_total,
        zstd_encode,
        zstd_decode,
    );
    print_large_row("DeltaStream", raw_total, ds_total, ds_encode, ds_decode);
    print_large_row(
        "DeltaStream + zstd",
        raw_total,
        dsz_total,
        dsz_encode,
        dsz_decode,
    );
    println!("\nFinal convergence   : PASS ✓");
    println!(
        "Total wall time     : {:.3} s",
        wall.elapsed().as_secs_f64()
    );
    Ok(())
}

fn print_large_row(
    name: &str,
    baseline: u64,
    bytes: u64,
    encode: std::time::Duration,
    decode: std::time::Duration,
) {
    let saving = (1.0 - bytes as f64 / baseline as f64) * 100.0;
    println!(
        "{:<22} {:>15} {:>9.2}% {:>12.3} {:>12.3}",
        name,
        bytes,
        saving,
        encode.as_secs_f64(),
        decode.as_secs_f64()
    );
}

fn pseudo_random_state(size: usize) -> Vec<u8> {
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

fn apply_sparse_packet(state: &mut Vec<u8>, packet: crate::Packet) -> Result<(), DeltaError> {
    let payload = packet.logical_payload()?;
    match packet.kind {
        PacketKind::Snapshot => {
            *state = payload;
        }
        PacketKind::Delta => {
            apply_sparse_byte_delta(state, &payload)?;
        }
    }
    Ok(())
}

fn apply_sparse_byte_delta(state: &mut [u8], payload: &[u8]) -> Result<(), DeltaError> {
    let mut cursor = 0usize;
    let count = read_var_u64(payload, &mut cursor)? as usize;
    let mut previous_index = 0usize;
    for n in 0..count {
        let index_delta = read_var_u64(payload, &mut cursor)? as usize;
        let index = if n == 0 {
            index_delta
        } else {
            previous_index.saturating_add(index_delta)
        };
        let value = *payload
            .get(cursor)
            .ok_or_else(|| DeltaError::InvalidPacket("truncated sparse delta".into()))?;
        cursor += 1;
        let slot = state
            .get_mut(index)
            .ok_or_else(|| DeltaError::InvalidPacket("sparse delta index out of range".into()))?;
        *slot = value;
        previous_index = index;
    }
    if cursor != payload.len() {
        return Err(DeltaError::InvalidPacket(
            "trailing sparse delta bytes".into(),
        ));
    }
    Ok(())
}

fn read_var_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, DeltaError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| DeltaError::InvalidPacket("truncated varint".into()))?;
        *cursor += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(DeltaError::InvalidPacket("varint overflow".into()));
        }
    }
}

fn validate_v25() -> Result<(), DeltaError> {
    let report = crate::v25_validation::run_release_validation()?;
    println!("=== DeltaStream V25 Release Validation ===");
    for (name, ok) in &report.checks {
        println!("{:<42} {}", name, if *ok { "PASS" } else { "FAIL" });
    }
    if report.all_passed() {
        println!("\nV25 validation: PASS ✓");
        Ok(())
    } else {
        Err(DeltaError::InvalidState("V25 validation failed"))
    }
}

fn edge_case_demo(include_large_limits: bool) -> Result<(), DeltaError> {
    let report = crate::edge_cases::run(include_large_limits)?;
    println!("=== DeltaStream V25 Edge-Case Suite ===");
    for (name, ok) in &report.checks {
        println!("{:<42} {}", name, if *ok { "PASS" } else { "FAIL" });
    }
    if report.all_passed() {
        println!("\nV25 edge cases: PASS ✓");
        Ok(())
    } else {
        Err(DeltaError::InvalidState("V25 edge-case suite failed"))
    }
}

fn torture_demo(clients: usize, updates: usize) -> Result<(), DeltaError> {
    use std::time::Instant;
    println!("=== DeltaStream V25 Torture Test ===");
    println!("clients             : {clients}");
    println!("updates             : {updates}");
    let started = Instant::now();
    let report = crate::torture::run(clients, updates)?;
    println!("deliveries          : {}", report.deliveries);
    println!("drops               : {}", report.drops);
    println!("duplicates          : {}", report.duplicates);
    println!("corruptions         : {}", report.corruptions);
    println!("reorders            : {}", report.reorders);
    println!("reorder drains      : {}", report.buffered_reorders);
    println!("disconnects         : {}", report.disconnects);
    println!("long disconnects    : {}", report.long_disconnects);
    println!("late joins          : {}", report.late_joins);
    println!("resyncs             : {}", report.resyncs);
    println!("storm clients       : {}", report.resync_storm_clients);
    println!("recovery snapshots  : {}", report.recovery_snapshots_built);
    println!("final sequence      : {}", report.final_sequence);
    println!(
        "clients converged   : {}/{}",
        report.converged_clients, report.clients
    );
    println!(
        "wall time           : {:.3} s",
        started.elapsed().as_secs_f64()
    );
    println!(
        "final convergence   : {}",
        if report.all_converged() {
            "PASS ✓"
        } else {
            "FAIL"
        }
    );
    if report.all_converged() {
        Ok(())
    } else {
        Err(DeltaError::InvalidState("V30 torture convergence failed"))
    }
}

fn validate_v20() -> Result<(), DeltaError> {
    let report = validation::run_release_validation()?;
    println!("=== DeltaStream V20 Release Validation ===");
    for (name, ok) in &report.checks {
        println!("{:<34} {}", name, if *ok { "PASS" } else { "FAIL" });
    }
    if report.all_passed() {
        println!("\nV20 validation: PASS ✓");
        Ok(())
    } else {
        Err(DeltaError::InvalidState("V20 validation failed"))
    }
}

fn chaos_demo(updates: usize) -> Result<(), DeltaError> {
    let report = chaos::run_deterministic(updates)?;
    println!("=== DeltaStream V30 Chaos/Soak Test ===");
    println!("updates generated   : {}", report.generated);
    println!("delivered/recovered : {}", report.delivered);
    println!("drops injected      : {}", report.intentionally_dropped);
    println!("duplicates injected : {}", report.duplicates_injected);
    println!("resyncs             : {}", report.resyncs);
    println!("final sequence      : {:?}", report.final_sequence);
    println!(
        "final convergence   : {}",
        if report.final_state_matches {
            "PASS ✓"
        } else {
            "FAIL"
        }
    );
    if report.final_state_matches {
        Ok(())
    } else {
        Err(DeltaError::InvalidState("chaos convergence failed"))
    }
}

fn capabilities_demo() -> Result<(), DeltaError> {
    println!(
        "{}",
        serde_json::to_string_pretty(&crate::compat::Capabilities::local())?
    );
    Ok(())
}

#[cfg(feature = "pubnub-transport")]
fn pubnub_publish_demo() -> Result<(), DeltaError> {
    use crate::{
        session::{PublisherSession, SessionTopics},
        transport::pubnub::{PubNubConfig, PubNubRealtimeTransport},
    };
    let mut config = PubNubConfig::from_env()?;
    config.user_id = format!("{}-publisher", config.user_id);
    let base = std::env::var("PUBNUB_CHANNEL").unwrap_or_else(|_| "delta-stream-demo".into());
    let runtime = tokio::runtime::Runtime::new().map_err(DeltaError::Io)?;
    runtime.block_on(async move {
        let transport = PubNubRealtimeTransport::new(config)?;
        let session =
            PublisherSession::new(transport, SessionTopics::new(base), AgentState::demo());
        let control = session.clone();
        tokio::spawn(async move {
            if let Err(e) = control.control_loop().await {
                eprintln!("control loop stopped: {e}");
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let mut state = AgentState::demo();
        for _ in 0..20 {
            let packet = session.publish_state(state.clone()).await?;
            println!(
                "published seq={} kind={:?} bytes={}",
                packet.sequence,
                packet.kind,
                packet.encoded_len()
            );
            state = state.advance();
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        println!("publisher waiting 2s for final ACK/resync traffic...");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        Ok(())
    })
}

#[cfg(feature = "pubnub-transport")]
fn pubnub_listen_demo() -> Result<(), DeltaError> {
    use crate::{
        session::{SessionTopics, SubscriberSession},
        transport::pubnub::{PubNubConfig, PubNubRealtimeTransport},
    };
    let mut config = PubNubConfig::from_env()?;
    config.user_id = format!("{}-subscriber", config.user_id);
    let base = std::env::var("PUBNUB_CHANNEL").unwrap_or_else(|_| "delta-stream-demo".into());
    let client_id =
        std::env::var("DELTASTREAM_CLIENT_ID").unwrap_or_else(|_| "demo-client-1".into());
    let runtime = tokio::runtime::Runtime::new().map_err(DeltaError::Io)?;
    runtime.block_on(async move {
        let transport = PubNubRealtimeTransport::new(config)?;
        println!("subscribing to {base}; demo intentionally drops delta seq=5 once");
        SubscriberSession::new(transport, SessionTopics::new(base), client_id)
            .run(Some(5))
            .await
    })
}

#[cfg(feature = "websocket-transport")]
fn websocket_broker_demo(addr: &str) -> Result<(), DeltaError> {
    let runtime = tokio::runtime::Runtime::new().map_err(DeltaError::Io)?;
    runtime.block_on(crate::ws_broker::run(addr))
}

fn print_help() {
    println!("DeltaStream V30");
    println!("  cargo run --release -- --validate-v30          full V30 release validation");
    println!(
        "  cargo run --release -- --v30-benchmark [N]     fast selector 100 KiB / 1% benchmark"
    );
    println!(
        "  cargo run --release -- --v30-torture            V30 fast-path 1,000 x 20,000 fault run"
    );
    println!(
        "  cargo run --release -- --v30-torture-max C U    configurable V30 fast-path torture"
    );
    println!("  cargo run --release -- --partial-repair        1 MiB chunk repair demo");
    println!(
        "  cargo run --release -- --validate-v25          inherited V25 regression validation"
    );
    println!(
        "  cargo run --release -- --edge-cases            hard edge cases incl. 64 MiB limits"
    );
    println!("  cargo run --release -- --torture               1,000 clients x 20,000 updates hard faults");
    println!("  cargo run --release -- --torture-max C U       configurable maximum torture run");
    println!(
        "  cargo run --release -- --user-scale            100/500/1000/2000 client scale matrix"
    );
    println!("  cargo run --release -- --smart-delta-benchmark 100 KiB x 100k high-entropy smart-delta test");
    println!(
        "  cargo run --release -- --strategy-compare      every V25 delta strategy raw + zstd"
    );
    println!("  cargo run --release -- --workload-matrix       sizes/change-rate adaptive matrix");
    println!(
        "  cargo run --release -- --recovery-matrix       replay-vs-snapshot recovery cost matrix"
    );
    println!("  cargo run --release -- --resync-storm [N]      shared-snapshot storm, default 10,000 clients");
    println!(
        "  cargo run --release -- --benchmark             AgentState adaptive codec benchmark"
    );
    println!("  cargo run --release -- --compare-benchmark     JSON/zstd/Patch/V25 comparison");
    println!("  cargo run --release -- --scenario-benchmark    sparse-state size/change matrix");
    println!("  cargo run --release -- --scale-benchmark       100,000,000-update scale test");
    println!("  cargo run --release -- --large-compare         10.24 GB high-entropy comparison");
    println!("  cargo run --release -- --multi-client          V20 100-client baseline simulation");
    println!(
        "  cargo run --release -- --chaos                 10,000-update deterministic chaos test"
    );
    println!(
        "  cargo run --release -- --soak                  100,000-update deterministic soak test"
    );
    println!("  cargo run -- --capabilities                    protocol/library capability JSON");
    println!("  cargo run -- --generic-demo                    generic #[derive(DeltaState)] demo");
    println!("  cargo run -- --serve                           dashboard on 127.0.0.1:8787");
    println!("  cargo test                                      correctness/property/edge tests");
    println!("  cargo bench                                     Criterion microbenchmarks");
    println!("  cargo run --features pubnub-transport -- --pubnub-listen");
    println!("  cargo run --features pubnub-transport -- --pubnub-publish");
    println!("  cargo run --features websocket-transport -- --ws-broker");
    println!("\nOptional adapters: websocket-transport, mqtt-transport, nats-transport, all-transports, full");
}
