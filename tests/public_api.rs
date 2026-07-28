use delta_stream::{Apply, DeltaState, Packet, PacketKind, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct PublicState {
    value: u64,
    label: String,
}

#[test]
fn public_api_roundtrips_state_and_wire_bytes() {
    let mut publisher = Publisher::<PublicState>::new();
    let mut subscriber = Subscriber::<PublicState>::new();

    let a = PublicState {
        value: 1,
        label: "ready".into(),
    };

    let b = PublicState {
        value: 2,
        label: "ready".into(),
    };

    let first = Packet::from_bytes(&publisher.update(&a).unwrap().to_bytes().unwrap()).unwrap();

    let second = Packet::from_bytes(&publisher.update(&b).unwrap().to_bytes().unwrap()).unwrap();

    assert!(matches!(
        subscriber.apply(first).unwrap(),
        Apply::Applied { sequence: 1, .. }
    ));

    assert!(matches!(
        subscriber.apply(second).unwrap(),
        Apply::Applied { sequence: 2, .. }
    ));

    assert_eq!(subscriber.state(), Some(&b));
}

#[test]
fn public_api_recovers_after_a_gap() {
    let mut publisher = Publisher::<PublicState>::new();
    let mut subscriber = Subscriber::<PublicState>::new();

    // Keep a sufficiently large unchanged field so that changing only `value`
    // produces a delta instead of an adaptive snapshot.
    let stable_label = "delta-stream-public-api-recovery-test-".repeat(256);

    let a = PublicState {
        value: 1,
        label: stable_label.clone(),
    };

    let b = PublicState {
        value: 2,
        label: stable_label.clone(),
    };

    let c = PublicState {
        value: 3,
        label: stable_label,
    };

    let initial = publisher.update(&a).unwrap();

    assert_eq!(initial.kind, PacketKind::Snapshot);

    assert!(matches!(
        subscriber.apply(initial).unwrap(),
        Apply::Applied { sequence: 1, .. }
    ));

    // Packet 2 is intentionally lost.
    let dropped = publisher.update(&b).unwrap();
    assert_eq!(dropped.kind, PacketKind::Delta);

    // Packet 3 depends on packet 2, so the subscriber must request recovery.
    let after_gap = publisher.update(&c).unwrap();
    assert_eq!(after_gap.kind, PacketKind::Delta);

    assert!(matches!(
        subscriber.apply(after_gap).unwrap(),
        Apply::NeedSnapshot {
            local_sequence: Some(1),
            required_sequence: 2,
        }
    ));

    // A recovery snapshot must represent the current sequence without
    // advancing the publisher's global sequence.
    let sequence_before = publisher.sequence();
    let recovery = publisher.recovery_snapshot(&c).unwrap();

    assert_eq!(recovery.kind, PacketKind::Snapshot);
    assert_eq!(recovery.sequence, sequence_before);
    assert_eq!(publisher.sequence(), sequence_before);

    assert!(matches!(
        subscriber.apply(recovery).unwrap(),
        Apply::Applied { sequence, .. } if sequence == sequence_before
    ));

    assert_eq!(subscriber.state(), Some(&c));
}
