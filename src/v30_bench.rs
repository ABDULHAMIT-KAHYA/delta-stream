use crate::{byte_sync::ByteStateDecoder, error::DeltaError, v30_sync::FastByteStateEncoder};
use std::time::Instant;

fn entropy_bytes(len: usize) -> Vec<u8> {
    let mut x = 0x9E3779B97F4A7C15u64;
    (0..len)
        .map(|_| {
            x ^= x << 7;
            x ^= x >> 9;
            x ^= x << 8;
            x as u8
        })
        .collect()
}

pub fn print_fast_encoder_benchmark(
    state_size: usize,
    updates: usize,
    change_percent: f64,
) -> Result<(), DeltaError> {
    println!("=== DeltaStream V30 Fast Encoder Benchmark ===");
    println!("state size       : {state_size} bytes");
    println!("updates          : {updates}");
    println!("change/update    : {change_percent:.3}%");
    let mut state = entropy_bytes(state_size);
    let mut enc = FastByteStateEncoder::new("v30/bench");
    let mut dec = ByteStateDecoder::new("v30/bench");
    let changes = ((state_size as f64 * change_percent / 100.0).round() as usize)
        .max(1)
        .min(state_size.max(1));
    let mut bytes = 0u64;
    let started = Instant::now();
    for update in 0..updates {
        if update > 0 {
            for n in 0..changes {
                let i = (update.wrapping_mul(97).wrapping_add(n.wrapping_mul(7919))) % state.len();
                state[i] ^= (update as u8).wrapping_add(n as u8).wrapping_add(1);
            }
        }
        let p = enc.encode(&state)?;
        bytes += p.encoded_len() as u64;
        let _ = dec.apply(p)?;
    }
    let elapsed = started.elapsed();
    let logical = state_size as u64 * updates as u64;
    let saving = if logical == 0 {
        0.0
    } else {
        (1.0 - bytes as f64 / logical as f64) * 100.0
    };
    println!("wire bytes        : {bytes}");
    println!("saving            : {saving:.2}%");
    println!("wall time         : {:.3} s", elapsed.as_secs_f64());
    println!(
        "updates/sec       : {:.0}",
        updates as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
    );
    println!(
        "avg candidates    : {:.2}",
        enc.metrics().selector_candidates as f64 / enc.metrics().encoded_packets.max(1) as f64
    );
    println!(
        "final convergence : {}",
        if dec.state() == Some(state.as_slice()) {
            "PASS ✓"
        } else {
            "FAIL"
        }
    );
    Ok(())
}

pub fn print_partial_repair_demo() -> Result<(), DeltaError> {
    use crate::partial_repair::PartialRepair;
    let base = entropy_bytes(1024 * 1024);
    let mut target = base.clone();
    for byte in target.iter_mut().take(132 * 1024).skip(128 * 1024) {
        *byte ^= 0x5A;
    }
    let repair = PartialRepair::build(&base, &target, 1024);
    let repaired = repair.apply(&base)?;
    println!("=== DeltaStream V30 Partial Repair ===");
    println!("state bytes       : {}", target.len());
    println!("changed chunks    : {}", repair.patches.len());
    println!("repair bytes      : {}", repair.payload_bytes());
    println!(
        "saving vs snapshot: {:.2}%",
        (1.0 - repair.payload_bytes() as f64 / target.len() as f64) * 100.0
    );
    println!(
        "exact repair      : {}",
        if repaired == target {
            "PASS ✓"
        } else {
            "FAIL"
        }
    );
    Ok(())
}
