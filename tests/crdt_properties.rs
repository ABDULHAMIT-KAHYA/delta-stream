#![cfg(feature = "crdt")]

use delta_stream::crdt::{Crdt, GCounter, LwwRegister, PNCounter, ReplicaId};
use proptest::prelude::*;

fn id(value: &str) -> ReplicaId {
    ReplicaId::new(value).unwrap()
}

fn gcounter_from(values: [u16; 3]) -> GCounter {
    let mut counter = GCounter::new();
    for (replica, value) in ["a", "b", "c"].into_iter().zip(values) {
        counter.increment(&id(replica), u64::from(value)).unwrap();
    }
    counter
}

fn pncounter_from(positive: [u16; 3], negative: [u16; 3]) -> PNCounter {
    let mut counter = PNCounter::new();
    for (replica, value) in ["a", "b", "c"].into_iter().zip(positive) {
        counter.increment(&id(replica), u64::from(value)).unwrap();
    }
    for (replica, value) in ["a", "b", "c"].into_iter().zip(negative) {
        counter.decrement(&id(replica), u64::from(value)).unwrap();
    }
    counter
}

fn assert_commutative<T>(a: T, b: T)
where
    T: Crdt + Clone + Eq + std::fmt::Debug,
{
    let mut left = a.clone();
    left.merge(&b);
    let mut right = b;
    right.merge(&a);
    assert_eq!(left, right);
}

fn assert_associative<T>(a: T, b: T, c: T)
where
    T: Crdt + Clone + Eq + std::fmt::Debug,
{
    let mut left = a.clone();
    left.merge(&b);
    left.merge(&c);

    let mut grouped = b;
    grouped.merge(&c);
    let mut right = a;
    right.merge(&grouped);

    assert_eq!(left, right);
}

fn assert_idempotent<T>(value: T)
where
    T: Crdt + Clone + Eq + std::fmt::Debug,
{
    let mut merged = value.clone();
    assert!(!merged.merge(&value));
    assert_eq!(merged, value);
}

proptest! {
    #[test]
    fn gcounter_merge_laws(a in any::<[u16; 3]>(), b in any::<[u16; 3]>(), c in any::<[u16; 3]>()) {
        let a = gcounter_from(a);
        let b = gcounter_from(b);
        let c = gcounter_from(c);
        assert_commutative(a.clone(), b.clone());
        assert_associative(a.clone(), b, c);
        assert_idempotent(a);
    }

    #[test]
    fn pncounter_merge_laws(
        ap in any::<[u16; 3]>(), an in any::<[u16; 3]>(),
        bp in any::<[u16; 3]>(), bn in any::<[u16; 3]>(),
        cp in any::<[u16; 3]>(), cn in any::<[u16; 3]>(),
    ) {
        let a = pncounter_from(ap, an);
        let b = pncounter_from(bp, bn);
        let c = pncounter_from(cp, cn);
        assert_commutative(a.clone(), b.clone());
        assert_associative(a.clone(), b, c);
        assert_idempotent(a);
    }

    #[test]
    fn lww_register_merge_laws(a_ts in 0_u64..1000, b_ts in 0_u64..1000, c_ts in 0_u64..1000) {
        let a = LwwRegister::new("a".to_string(), a_ts, id("a"));
        let b = LwwRegister::new("b".to_string(), b_ts, id("b"));
        let c = LwwRegister::new("c".to_string(), c_ts, id("c"));
        assert_commutative(a.clone(), b.clone());
        assert_associative(a.clone(), b, c);
        assert_idempotent(a);
    }
}

#[test]
fn gcounter_replicas_converge_after_duplicate_and_reordered_delivery() {
    let mut replicas = [GCounter::new(), GCounter::new(), GCounter::new()];
    replicas[0].increment(&id("a"), 1).unwrap();
    replicas[1].increment(&id("b"), 2).unwrap();
    replicas[2].increment(&id("c"), 3).unwrap();

    let messages = [
        replicas[2].clone(),
        replicas[0].clone(),
        replicas[2].clone(),
        replicas[1].clone(),
        replicas[0].clone(),
        replicas[1].clone(),
    ];

    for replica in &mut replicas {
        for message in &messages {
            replica.merge(message);
        }
    }

    assert_eq!(replicas[0], replicas[1]);
    assert_eq!(replicas[1], replicas[2]);
    assert_eq!(replicas[0].value().unwrap(), 6);
}

#[test]
fn pncounter_fault_style_three_replica_simulation_converges() {
    let mut a = PNCounter::new();
    let mut b = PNCounter::new();
    let mut c = PNCounter::new();

    a.increment(&id("a"), 10).unwrap();
    b.decrement(&id("b"), 4).unwrap();
    c.increment(&id("c"), 2).unwrap();

    let delayed_and_duplicated = [
        c.clone(),
        a.clone(),
        c.clone(),
        b.clone(),
        a.clone(),
        b.clone(),
    ];

    for message in &delayed_and_duplicated {
        a.merge(message);
    }
    for message in delayed_and_duplicated.iter().rev() {
        b.merge(message);
    }
    for message in [
        &delayed_and_duplicated[1],
        &delayed_and_duplicated[3],
        &delayed_and_duplicated[0],
        &delayed_and_duplicated[3],
    ] {
        c.merge(message);
    }

    assert_eq!(a, b);
    assert_eq!(b, c);
    assert_eq!(a.value().unwrap(), 8);
}

#[test]
fn lww_register_fault_style_three_replica_simulation_converges() {
    let a = LwwRegister::new("a".to_string(), 1, id("a"));
    let b = LwwRegister::new("b".to_string(), 2, id("b"));
    let c = LwwRegister::new("c".to_string(), 2, id("c"));
    let messages = [c.clone(), a.clone(), b.clone(), c.clone(), a.clone()];

    let mut left = a.clone();
    let mut middle = b.clone();
    let mut right = c.clone();

    for message in &messages {
        left.merge(message);
    }
    for message in messages.iter().rev() {
        middle.merge(message);
    }
    for message in [&messages[1], &messages[2], &messages[0], &messages[2]] {
        right.merge(message);
    }

    assert_eq!(left, middle);
    assert_eq!(middle, right);
    assert_eq!(left.value(), "c");
}
