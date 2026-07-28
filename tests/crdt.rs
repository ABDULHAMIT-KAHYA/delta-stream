#![cfg(feature = "crdt")]

use delta_stream::crdt::{
    decode_crdt, encode_crdt, Crdt, CrdtDecodeConfig, CrdtError, GCounter, LwwRegister, PNCounter,
    ReplicaId,
};

fn id(value: &str) -> ReplicaId {
    ReplicaId::new(value).unwrap()
}

#[test]
fn replica_id_accepts_valid_identifiers() {
    let replica = ReplicaId::new("replica-a").unwrap();
    assert_eq!(replica.as_str(), "replica-a");
    assert_eq!(replica.to_string(), "replica-a");
}

#[test]
fn replica_id_rejects_empty_identifier() {
    assert!(matches!(ReplicaId::new(""), Err(CrdtError::EmptyReplicaId)));
}

#[test]
fn replica_id_has_deterministic_ordering() {
    assert!(id("a") < id("b"));
}

#[test]
fn replica_id_serialization_round_trip() {
    let replica = id("replica-a");
    let bytes = encode_crdt(&replica).unwrap();
    assert_eq!(
        decode_crdt::<ReplicaId>(&bytes, CrdtDecodeConfig::default()).unwrap(),
        replica
    );
}

#[test]
fn gcounter_starts_at_zero() {
    assert_eq!(GCounter::new().value().unwrap(), 0);
}

#[test]
fn gcounter_increment_updates_only_the_correct_component() {
    let a = id("a");
    let b = id("b");
    let mut counter = GCounter::new();
    counter.increment(&a, 4).unwrap();
    assert_eq!(counter.component(&a), 4);
    assert_eq!(counter.component(&b), 0);
}

#[test]
fn gcounter_multiple_replicas_sum_correctly() {
    let mut counter = GCounter::new();
    counter.increment(&id("a"), 4).unwrap();
    counter.increment(&id("b"), 5).unwrap();
    assert_eq!(counter.value().unwrap(), 9);
}

#[test]
fn gcounter_merge_takes_component_wise_maximum() {
    let a = id("a");
    let b = id("b");
    let mut left = GCounter::new();
    let mut right = GCounter::new();
    left.increment(&a, 5).unwrap();
    left.increment(&b, 1).unwrap();
    right.increment(&a, 2).unwrap();
    right.increment(&b, 7).unwrap();

    assert!(left.merge(&right));
    assert_eq!(left.component(&a), 5);
    assert_eq!(left.component(&b), 7);
}

#[test]
fn gcounter_duplicate_merge_is_idempotent() {
    let mut left = GCounter::new();
    let mut right = GCounter::new();
    right.increment(&id("b"), 7).unwrap();
    assert!(left.merge(&right));
    assert!(!left.merge(&right));
}

#[test]
fn gcounter_merge_reports_whether_state_changed() {
    let mut left = GCounter::new();
    let mut right = GCounter::new();
    let replica = id("a");
    left.increment(&replica, 3).unwrap();
    right.increment(&replica, 2).unwrap();
    assert!(!left.merge(&right));
    right.increment(&replica, 2).unwrap();
    assert!(left.merge(&right));
}

#[test]
fn gcounter_increment_overflow_returns_error() {
    let replica = id("a");
    let mut counter = GCounter::new();
    counter.increment(&replica, u64::MAX).unwrap();
    assert!(matches!(
        counter.increment(&replica, 1),
        Err(CrdtError::CounterOverflow)
    ));
}

#[test]
fn gcounter_total_overflow_returns_error() {
    let mut counter = GCounter::new();
    counter.increment(&id("a"), u64::MAX).unwrap();
    counter.increment(&id("b"), 1).unwrap();
    assert!(matches!(counter.value(), Err(CrdtError::ValueOverflow)));
}

#[test]
fn gcounter_serialization_round_trip() {
    let mut counter = GCounter::new();
    counter.increment(&id("a"), 2).unwrap();
    let bytes = encode_crdt(&counter).unwrap();
    assert_eq!(
        decode_crdt::<GCounter>(&bytes, CrdtDecodeConfig::default()).unwrap(),
        counter
    );
}

#[test]
fn gcounter_two_replicas_converge() {
    let mut left = GCounter::new();
    let mut right = GCounter::new();
    left.increment(&id("a"), 2).unwrap();
    right.increment(&id("b"), 3).unwrap();
    let left_state = left.clone();
    let right_state = right.clone();
    left.merge(&right_state);
    right.merge(&left_state);
    assert_eq!(left, right);
}

#[test]
fn pncounter_starts_at_zero() {
    assert_eq!(PNCounter::new().value().unwrap(), 0);
}

#[test]
fn pncounter_increments_and_decrements() {
    let replica = id("a");
    let mut counter = PNCounter::new();
    counter.increment(&replica, 10).unwrap();
    counter.decrement(&replica, 3).unwrap();
    assert_eq!(counter.value().unwrap(), 7);
}

#[test]
fn pncounter_supports_negative_values() {
    let mut counter = PNCounter::new();
    counter.decrement(&id("a"), 5).unwrap();
    assert_eq!(counter.value().unwrap(), -5);
}

#[test]
fn pncounter_independent_replicas_merge_sides() {
    let mut left = PNCounter::new();
    let mut right = PNCounter::new();
    left.increment(&id("a"), 10).unwrap();
    right.decrement(&id("b"), 4).unwrap();
    assert!(left.merge(&right));
    assert_eq!(left.value().unwrap(), 6);
}

#[test]
fn pncounter_duplicate_merge_is_idempotent() {
    let mut left = PNCounter::new();
    let mut right = PNCounter::new();
    right.decrement(&id("b"), 4).unwrap();
    assert!(left.merge(&right));
    assert!(!left.merge(&right));
}

#[test]
fn pncounter_overflow_handling() {
    let mut counter = PNCounter::new();
    counter.increment(&id("a"), u64::MAX).unwrap();
    counter.increment(&id("b"), 1).unwrap();
    assert!(matches!(counter.value(), Err(CrdtError::ValueOverflow)));
}

#[test]
fn pncounter_serialization_round_trip() {
    let mut counter = PNCounter::new();
    counter.increment(&id("a"), 2).unwrap();
    counter.decrement(&id("b"), 1).unwrap();
    let bytes = encode_crdt(&counter).unwrap();
    assert_eq!(
        decode_crdt::<PNCounter>(&bytes, CrdtDecodeConfig::default()).unwrap(),
        counter
    );
}

#[test]
fn pncounter_converges_after_opposite_delivery_orders() {
    let mut a = PNCounter::new();
    let mut b = PNCounter::new();
    let mut c = PNCounter::new();
    a.increment(&id("a"), 5).unwrap();
    b.decrement(&id("b"), 2).unwrap();
    c.increment(&id("c"), 3).unwrap();

    let states = [a.clone(), b.clone(), c.clone()];
    for state in &states {
        a.merge(state);
    }
    for state in states.iter().rev() {
        b.merge(state);
    }
    c.merge(&states[1]);
    c.merge(&states[0]);

    assert_eq!(a, b);
    assert_eq!(b, c);
    assert_eq!(a.value().unwrap(), 6);
}

#[test]
fn lww_register_newer_timestamp_wins() {
    let mut register = LwwRegister::new("old", 1, id("a"));
    assert!(register.assign("new", 2, id("a")));
    assert_eq!(register.value(), &"new");
}

#[test]
fn lww_register_older_timestamp_loses() {
    let mut register = LwwRegister::new("new", 2, id("a"));
    assert!(!register.assign("old", 1, id("b")));
    assert_eq!(register.value(), &"new");
}

#[test]
fn lww_register_equal_timestamp_uses_replica_id_tie_break() {
    let mut register = LwwRegister::new("a", 7, id("a"));
    assert!(register.assign("b", 7, id("b")));
    assert_eq!(register.value(), &"b");
}

#[test]
fn lww_register_duplicate_merge_is_idempotent() {
    let mut left = LwwRegister::new("a", 1, id("a"));
    let right = LwwRegister::new("b", 2, id("b"));
    assert!(left.merge(&right));
    assert!(!left.merge(&right));
}

#[test]
fn lww_register_merge_order_does_not_alter_result() {
    let a = LwwRegister::new("a", 1, id("a"));
    let b = LwwRegister::new("b", 1, id("b"));
    let mut left = a.clone();
    let mut right = b.clone();
    left.merge(&b);
    right.merge(&a);
    assert_eq!(left, right);
}

#[test]
fn lww_register_exact_same_metadata_does_not_change_state() {
    let mut register = LwwRegister::new("a", 1, id("a"));
    assert!(!register.assign("ignored", 1, id("a")));
    assert_eq!(register.value(), &"a");
}

#[test]
fn lww_register_serialization_round_trip() {
    let register = LwwRegister::new("value".to_string(), 3, id("a"));
    let bytes = encode_crdt(&register).unwrap();
    assert_eq!(
        decode_crdt::<LwwRegister<String>>(&bytes, CrdtDecodeConfig::default()).unwrap(),
        register
    );
}

#[test]
fn crdt_decode_rejects_oversized_input() {
    let bytes = encode_crdt(&GCounter::new()).unwrap();
    assert!(matches!(
        decode_crdt::<GCounter>(
            &bytes,
            CrdtDecodeConfig {
                max_encoded_bytes: bytes.len() - 1
            }
        ),
        Err(CrdtError::EncodedValueTooLarge { .. })
    ));
}
