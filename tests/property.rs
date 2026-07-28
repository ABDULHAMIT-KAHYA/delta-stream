use delta_stream::{AgentState, ApplyResult, Decoder, Encoder};
use proptest::prelude::*;

proptest! {
    #[test]
    fn arbitrary_numeric_changes_reconstruct(
        progress in 0u8..=100,
        tokens in 0u64..10_000_000,
        memory in 0u64..1_000_000,
        cpu in 0f32..100f32,
        files in 0u64..1_000_000,
    ) {
        let a = AgentState::demo();
        let mut b = a.clone();
        b.progress = progress;
        b.tokens = tokens;
        b.memory_mb = memory;
        b.cpu_percent = cpu;
        b.files_processed = files;

        let mut enc = Encoder::default();
        let mut dec = Decoder::default();
        let snapshot = enc.encode(&a).unwrap();
        let delta = enc.encode(&b).unwrap();
        let _ = dec.apply_packet(snapshot).unwrap();
        match dec.apply_packet(delta).unwrap() {
            ApplyResult::Applied { state, .. } => prop_assert_eq!(state, b),
            other => prop_assert!(false, "unexpected result: {:?}", other),
        }
    }
}
