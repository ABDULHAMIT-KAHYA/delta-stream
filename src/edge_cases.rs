use crate::{
    byte_sync::{ByteApplyResult, ByteStateDecoder, ByteStateEncoder},
    error::DeltaError,
    packet::{Packet, MAX_WIRE_PAYLOAD},
    recovery_history::{RecoveryHistory, RecoveryPlan},
    reorder::{ReorderApplyResult, ReorderDecoder},
    smart_delta,
    state::AgentState,
    sync::Encoder,
};

#[derive(Debug, Clone, Default)]
pub struct EdgeCaseReport {
    pub checks: Vec<(&'static str, bool)>,
}

impl EdgeCaseReport {
    pub fn all_passed(&self) -> bool {
        self.checks.iter().all(|(_, ok)| *ok)
    }
}

pub fn run(include_large_limits: bool) -> Result<EdgeCaseReport, DeltaError> {
    let mut checks = Vec::new();

    // Empty state.
    let mut enc = ByteStateEncoder::new("edge/bytes/v25");
    let mut dec = ByteStateDecoder::new("edge/bytes/v25");
    let empty = enc.encode(&[])?;
    checks.push((
        "zero-length state",
        matches!(dec.apply(empty)?, ByteApplyResult::Applied { state, .. } if state.is_empty()),
    ));

    // One byte and identical update.
    let one = enc.encode(&[7])?;
    checks.push((
        "one-byte grow",
        matches!(dec.apply(one)?, ByteApplyResult::Applied { state, .. } if state == vec![7]),
    ));
    let identical = enc.encode(&[7])?;
    checks.push((
        "identical update",
        matches!(dec.apply(identical)?, ByteApplyResult::Applied { state, .. } if state == vec![7]),
    ));

    // Insert/delete/resize exercise splice/chunk-safe semantics.
    let a = b"abcdefghij".to_vec();
    let b = b"abcXYZdefghij".to_vec();
    let c = b"abghij".to_vec();
    let mut e2 = ByteStateEncoder::new("edge/resize/v25");
    let mut d2 = ByteStateDecoder::new("edge/resize/v25");
    let _ = d2.apply(e2.encode(&a)?)?;
    let inserted = d2.apply(e2.encode(&b)?)?;
    checks.push((
        "middle insertion",
        matches!(inserted, ByteApplyResult::Applied { state, .. } if state == b),
    ));
    let deleted = d2.apply(e2.encode(&c)?)?;
    checks.push((
        "shrink/delete",
        matches!(deleted, ByteApplyResult::Applied { state, .. } if state == c),
    ));

    // Every smart delta candidate must reconstruct exactly.
    let previous = b"0123456789abcdefghijklmnop".to_vec();
    let mut current = previous.clone();
    current[2] ^= 0x55;
    current[3] ^= 0x33;
    current[20] ^= 0x77;
    let candidates = smart_delta::encode_candidates(&previous, &current, Default::default())?;
    let all_reconstruct = candidates.iter().all(|candidate| {
        smart_delta::apply(&previous, &candidate.payload)
            .ok()
            .as_deref()
            == Some(current.as_slice())
    });
    checks.push(("all smart delta strategies reconstruct", all_reconstruct));

    // Malformed smart delta.
    checks.push((
        "malformed smart delta rejected",
        smart_delta::apply(&previous, &[255, 1, 2, 3]).is_err(),
    ));

    // Schema mismatch.
    let mut wrong = ByteStateDecoder::new("wrong/schema/v25");
    let packet = e2.recovery_snapshot(&c)?;
    checks.push(("schema mismatch rejected", wrong.apply(packet).is_err()));

    // Future delta before a base snapshot must not apply.
    let mut future_enc = ByteStateEncoder::new("future/v25");
    let base = vec![0u8; 4096];
    let mut next = base.clone();
    next[2048] = 1;
    let _p1 = future_enc.encode(&base)?;
    let p2 = future_enc.encode(&next)?;
    let is_delta = matches!(p2.kind, crate::packet::PacketKind::Delta);
    let mut future_dec = ByteStateDecoder::new("future/v25");
    let requires_recovery = matches!(future_dec.apply(p2)?, ByteApplyResult::NeedRecovery { .. });
    checks.push((
        "future delta requires recovery",
        is_delta && requires_recovery,
    ));

    // Explicit stream reset supports publisher process restart / sequence restart.
    let mut restart_encoder_a = ByteStateEncoder::new("restart/v25");
    let mut restart_decoder = ByteStateDecoder::new("restart/v25");
    let _ = restart_decoder.apply(restart_encoder_a.encode(b"before-restart")?)?;
    let mut restart_encoder_b = ByteStateEncoder::new("restart/v25");
    let restarted = restart_encoder_b.encode(b"after-restart")?;
    let ignored_without_reset = matches!(
        restart_decoder.apply(restarted.clone())?,
        ByteApplyResult::Duplicate { .. }
    );
    restart_decoder.reset();
    let accepted_after_reset = matches!(restart_decoder.apply(restarted)?, ByteApplyResult::Applied { state, .. } if state == b"after-restart".to_vec());
    checks.push((
        "publisher restart requires explicit reset",
        ignored_without_reset && accepted_after_reset,
    ));

    // Old snapshot arriving after newer state must not roll state backward.
    let mut normal_enc = ByteStateEncoder::new("stale/v25");
    let old = normal_enc.encode(b"old")?;
    let new = normal_enc.encode(b"new")?;
    let mut normal_dec = ByteStateDecoder::new("stale/v25");
    let _ = normal_dec.apply(old.clone())?;
    let _ = normal_dec.apply(new)?;
    checks.push((
        "late old snapshot suppressed",
        matches!(normal_dec.apply(old)?, ByteApplyResult::Duplicate { .. })
            && normal_dec.state() == Some(b"new".as_slice()),
    ));

    // Corrupted compressed payload must fail decompression or checksum.
    let raw = Packet::snapshot(1, 1, 1, vec![0u8; 4096]);
    let compressed = raw.zstd_candidate(1, 1)?;
    let mut wire = compressed.encode()?;
    if wire.len() > 50 {
        wire[50] ^= 0x5a;
    }
    checks.push((
        "corrupted compressed packet rejected",
        Packet::decode(&wire).is_err(),
    ));

    // Recovery history: tiny contiguous gap chooses replay; large/missing gap chooses snapshot.
    let mut agent_encoder = Encoder::default();
    let mut history = RecoveryHistory::new(16, 1024 * 1024);
    let mut agent = AgentState::demo();
    let first = agent_encoder.encode(&agent)?;
    history.record(first);
    for _ in 0..5 {
        agent = agent.advance();
        history.record(agent_encoder.encode(&agent)?);
    }
    let snapshot = agent_encoder.recovery_snapshot(&agent)?;
    let plan = history.plan(
        Some(agent_encoder.sequence().saturating_sub(2)),
        snapshot.clone(),
    )?;
    checks.push((
        "small recovery gap can replay",
        matches!(plan, RecoveryPlan::Replay(ref packets) if packets.len() == 2),
    ));
    let plan = history.plan(Some(0), snapshot)?;
    checks.push((
        "large recovery gap falls back snapshot",
        matches!(plan, RecoveryPlan::Snapshot(_)),
    ));

    // Reorder buffer: seq 3 before seq 2 should buffer and then drain.
    let mut oe = Encoder::default();
    let s1 = AgentState::demo();
    let s2 = s1.advance();
    let s3 = s2.advance();
    let op1 = oe.encode(&s1)?;
    let op2 = oe.encode(&s2)?;
    let op3 = oe.encode(&s3)?;
    let mut reorder = ReorderDecoder::new(4, 8);
    let _ = reorder.apply(op1)?;
    let buffered = reorder.apply(op3)?;
    let drained = reorder.apply(op2)?;
    checks.push((
        "bounded reorder buffers future packet",
        matches!(buffered, ReorderApplyResult::Buffered { .. }),
    ));
    checks.push((
        "reorder drains without rollback",
        matches!(drained, ReorderApplyResult::Applied { drained: 1, .. })
            && reorder.state() == Some(&s3),
    ));

    // Maximum sequence value must survive the wire format unchanged.
    let max_seq = Packet::snapshot(u64::MAX, 1, 1, vec![1, 2, 3]);
    let decoded_max = Packet::decode(&max_seq.encode()?)?;
    checks.push((
        "u64::MAX sequence roundtrip",
        decoded_max.sequence == u64::MAX,
    ));

    // Exact and over-limit payloads. This is optional because it allocates large buffers.
    if include_large_limits {
        let exact = Packet::snapshot(1, 1, 1, vec![0u8; MAX_WIRE_PAYLOAD]);
        checks.push(("64 MiB exact wire payload accepted", exact.encode().is_ok()));
        let over = Packet::snapshot(1, 1, 1, vec![0u8; MAX_WIRE_PAYLOAD + 1]);
        checks.push(("64 MiB + 1 rejected", over.encode().is_err()));

        // A highly compressible logical payload larger than the decompression cap can
        // fit on the wire. logical_payload() must still reject it after decompression.
        let bomb_source = vec![0u8; MAX_WIRE_PAYLOAD + 1];
        let bomb = Packet::snapshot(1, 1, 1, bomb_source).zstd_candidate(1, 1)?;
        checks.push((
            "decompression limit enforced",
            bomb.is_compressed() && bomb.logical_payload().is_err(),
        ));
    }

    Ok(EdgeCaseReport { checks })
}
