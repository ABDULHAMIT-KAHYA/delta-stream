use delta_stream::{smart_delta, ByteApplyResult, ByteStateDecoder, ByteStateEncoder, Packet};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_equal_length_states_reconstruct(
        previous in prop::collection::vec(any::<u8>(), 0..4096),
        mutations in prop::collection::vec((0usize..4096, any::<u8>()), 0..128),
    ) {
        let mut current = previous.clone();
        if !current.is_empty() {
            for (index, value) in mutations {
                let len = current.len();
                current[index % len] = value;
            }
        }
        let candidates = smart_delta::encode_candidates(&previous, &current, Default::default()).unwrap();
        for candidate in candidates {
            prop_assert_eq!(smart_delta::apply(&previous, &candidate.payload).unwrap(), current.clone());
        }
    }

    #[test]
    fn arbitrary_resize_splice_reconstructs(
        previous in prop::collection::vec(any::<u8>(), 0..2048),
        current in prop::collection::vec(any::<u8>(), 0..2048),
    ) {
        let candidates = smart_delta::encode_candidates(&previous, &current, Default::default()).unwrap();
        prop_assert!(candidates.iter().any(|c| smart_delta::apply(&previous, &c.payload).ok().as_deref() == Some(current.as_slice())));
    }

    #[test]
    fn packet_decoder_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..8192)) {
        let _ = Packet::decode(&bytes);
    }

    #[test]
    fn byte_stream_arbitrary_sequence_converges(
        states in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..1024), 1..32)
    ) {
        let mut encoder = ByteStateEncoder::new("property/v25");
        let mut decoder = ByteStateDecoder::new("property/v25");
        for state in states {
            let packet = encoder.encode(&state).unwrap();
            match decoder.apply(packet).unwrap() {
                ByteApplyResult::Applied { state: got, .. } => prop_assert_eq!(got, state),
                other => prop_assert!(false, "unexpected apply result: {other:?}"),
            }
        }
    }
}
