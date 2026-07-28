use delta_stream::{
    Apply, DecodeConfig, DeltaError, DeltaState, Packet, PacketKind, Publisher, Subscriber,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct PlayerState {
    name: String,
    x: i32,
    y: i32,
    health: u16,
    inventory: String,
}

fn state(x: i32, health: u16) -> PlayerState {
    PlayerState {
        name: "Abdulhamit".into(),
        x,
        y: 20,
        health,
        inventory: "stable-inventory-field-".repeat(256),
    }
}

#[test]
fn encode_receive_initial_snapshot() {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();
    let initial = state(10, 100);

    let bytes = publisher.encode(&initial).unwrap();
    let result = subscriber.receive(&bytes).unwrap();

    assert!(matches!(result, Apply::Applied { sequence: 1, .. }));
    assert_eq!(subscriber.state(), Some(&initial));
}

#[test]
fn encode_receive_delta_update() {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();
    let initial = state(10, 100);
    let updated = state(11, 98);

    subscriber
        .receive(&publisher.encode(&initial).unwrap())
        .unwrap();
    let packet = publisher.update(&updated).unwrap();
    assert_eq!(packet.kind, PacketKind::Delta);
    let bytes = packet.to_bytes().unwrap();
    let result = subscriber.receive(&bytes).unwrap();

    assert!(matches!(result, Apply::Applied { sequence: 2, .. }));
    assert_eq!(subscriber.state(), Some(&updated));
}

#[test]
fn receive_detects_duplicate() {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();
    let bytes = publisher.encode(&state(10, 100)).unwrap();

    subscriber.receive(&bytes).unwrap();
    let duplicate = subscriber.receive(&bytes).unwrap();

    assert_eq!(duplicate, Apply::Duplicate { sequence: 1 });
}

#[test]
fn receive_detects_gap_and_requests_snapshot() {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();

    subscriber
        .receive(&publisher.encode(&state(10, 100)).unwrap())
        .unwrap();
    let _dropped = publisher.encode(&state(11, 99)).unwrap();
    let later = publisher.encode(&state(12, 98)).unwrap();

    assert!(matches!(
        subscriber.receive(&later).unwrap(),
        Apply::NeedSnapshot {
            local_sequence: Some(1),
            required_sequence: 2
        }
    ));
}

#[test]
fn recovery_snapshot_restores_state() {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();
    let initial = state(10, 100);
    let missed = state(11, 99);
    let current = state(12, 98);

    subscriber
        .receive(&publisher.encode(&initial).unwrap())
        .unwrap();
    let _dropped = publisher.encode(&missed).unwrap();
    let later = publisher.encode(&current).unwrap();
    assert!(matches!(
        subscriber.receive(&later).unwrap(),
        Apply::NeedSnapshot { .. }
    ));

    let recovery = publisher.recovery_snapshot(&current).unwrap();
    subscriber.apply(recovery).unwrap();

    assert_eq!(subscriber.state(), Some(&current));
}

#[test]
fn receive_rejects_truncated_packet() {
    let mut publisher = Publisher::<PlayerState>::new();
    let bytes = publisher.encode(&state(10, 100)).unwrap();

    for cut in [0, 1, 2, 10, 45, bytes.len() - 1] {
        let result = std::panic::catch_unwind(|| Packet::from_bytes(&bytes[..cut]));
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());
    }
}

#[test]
fn receive_rejects_corrupted_packet() {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();
    let mut bytes = publisher.encode(&state(10, 100)).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;

    assert!(matches!(
        subscriber.receive(&bytes),
        Err(DeltaError::ChecksumMismatch { .. })
    ));
    assert_eq!(subscriber.state(), None);
}

#[test]
fn receive_rejects_oversized_packet() {
    let mut publisher = Publisher::<PlayerState>::new();
    let bytes = publisher.encode(&state(10, 100)).unwrap();
    let mut subscriber = Subscriber::<PlayerState>::with_decode_config(DecodeConfig {
        max_packet_bytes: bytes.len() - 1,
        max_decompressed_bytes: 64 * 1024,
    });

    assert!(matches!(
        subscriber.receive(&bytes),
        Err(DeltaError::PacketTooLarge { .. })
    ));
}

#[test]
fn receive_rejects_oversized_decompressed_payload() {
    let logical = vec![b'a'; 1024];
    let compressed = zstd::bulk::compress(&logical, 1).unwrap();
    let mut packet = Packet::snapshot(
        1,
        delta_stream::sync::fnv1a64(&logical),
        PlayerState::schema_hash(),
        compressed,
    );
    packet.flags = delta_stream::packet::FLAG_COMPRESSED_ZSTD;
    let bytes = packet.to_bytes().unwrap();
    let mut subscriber = Subscriber::<PlayerState>::with_decode_config(DecodeConfig {
        max_packet_bytes: 64 * 1024,
        max_decompressed_bytes: 16,
    });

    assert!(subscriber.receive(&bytes).is_err());
    assert_eq!(subscriber.state(), None);
}

#[test]
fn published_style_external_api_compiles() {
    use delta_stream::{DeltaState, Publisher, Subscriber};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
    struct PublicStyle {
        value: u64,
    }

    let mut publisher = Publisher::<PublicStyle>::new();
    let mut subscriber = Subscriber::<PublicStyle>::new();
    let state = PublicStyle { value: 7 };
    let bytes = publisher.encode(&state).unwrap();
    subscriber.receive(&bytes).unwrap();
    assert_eq!(subscriber.state(), Some(&state));
}

#[test]
fn high_level_and_low_level_paths_are_equivalent() {
    let state_a = state(10, 100);
    let state_b = state(11, 98);
    let mut high_pub = Publisher::<PlayerState>::new();
    let mut high_sub = Subscriber::<PlayerState>::new();
    let mut low_pub = Publisher::<PlayerState>::new();
    let mut low_sub = Subscriber::<PlayerState>::new();

    let high_a = high_pub.encode(&state_a).unwrap();
    let low_a_packet = low_pub.update(&state_a).unwrap();
    let low_a = low_a_packet.to_bytes().unwrap();
    assert_eq!(high_a, low_a);
    assert_eq!(
        high_sub.receive(&high_a).unwrap(),
        low_sub.apply(Packet::from_bytes(&low_a).unwrap()).unwrap()
    );

    let high_b = high_pub.encode(&state_b).unwrap();
    let low_b_packet = low_pub.update(&state_b).unwrap();
    let low_b = low_b_packet.to_bytes().unwrap();
    assert_eq!(high_b, low_b);
    assert_eq!(
        high_sub.receive(&high_b).unwrap(),
        low_sub.apply(Packet::from_bytes(&low_b).unwrap()).unwrap()
    );
    assert_eq!(high_sub.state(), low_sub.state());
}

#[test]
fn malformed_input_never_panics() {
    let mut invalid_declared_len = b"DS\x03\x01\x00\x00".to_vec();
    invalid_declared_len.resize(46, 0);
    invalid_declared_len[42..46].copy_from_slice(&10_u32.to_le_bytes());

    let corpus: Vec<Vec<u8>> = vec![
        vec![],
        vec![0],
        b"NO".to_vec(),
        b"DS\xff".to_vec(),
        b"DS\x03\xff".to_vec(),
        invalid_declared_len,
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    ];

    for data in corpus {
        let result = std::panic::catch_unwind(|| Packet::from_bytes(&data));
        assert!(result.is_ok());
    }
}

#[test]
fn recovery_snapshot_does_not_create_sequence_gap() {
    let mut publisher = Publisher::<PlayerState>::new();
    let current = state(12, 98);
    publisher.encode(&state(10, 100)).unwrap();
    publisher.encode(&current).unwrap();
    let before = publisher.sequence();

    let recovery = publisher.recovery_snapshot(&current).unwrap();

    assert_eq!(recovery.sequence, before);
    assert_eq!(publisher.sequence(), before);
}

#[test]
fn state_remains_unchanged_after_rejected_packet() {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();
    let initial = state(10, 100);

    subscriber
        .receive(&publisher.encode(&initial).unwrap())
        .unwrap();
    let mut corrupted = publisher.encode(&state(11, 98)).unwrap();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xff;

    assert!(subscriber.receive(&corrupted).is_err());
    assert_eq!(subscriber.state(), Some(&initial));
}
