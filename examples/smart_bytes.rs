use delta_stream::{ByteApplyResult, ByteStateDecoder, ByteStateEncoder};

fn main() -> Result<(), delta_stream::DeltaError> {
    let mut publisher = ByteStateEncoder::new("example/SmartBytes/v1");
    let mut subscriber = ByteStateDecoder::new("example/SmartBytes/v1");

    let mut state = vec![0u8; 32 * 1024];
    let first = publisher.encode(&state)?;
    let _ = subscriber.apply(first)?;

    for i in 0..100 {
        state[(i * 97) % (32 * 1024)] = i as u8;
        let packet = publisher.encode(&state)?;
        match subscriber.apply(packet)? {
            ByteApplyResult::Applied {
                state: reconstructed,
                ..
            } => {
                assert_eq!(reconstructed, state);
            }
            other => panic!("unexpected apply result: {other:?}"),
        }
    }

    println!("V25 smart byte-state example converged");
    Ok(())
}
