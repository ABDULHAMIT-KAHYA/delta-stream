use delta_stream::crdt::{Crdt, GCounter, ReplicaId};

fn main() -> Result<(), delta_stream::crdt::CrdtError> {
    let replica_a = ReplicaId::new("replica-a")?;
    let replica_b = ReplicaId::new("replica-b")?;

    let mut counter_a = GCounter::new();
    let mut counter_b = GCounter::new();

    counter_a.increment(&replica_a, 3)?;
    counter_b.increment(&replica_b, 4)?;

    counter_a.merge(&counter_b);
    counter_b.merge(&counter_a);

    let changed_by_duplicate = counter_a.merge(&counter_b);

    assert!(!changed_by_duplicate);
    assert_eq!(counter_a, counter_b);
    assert_eq!(counter_a.value()?, 7);

    println!(
        "counter replicas converged with value {}",
        counter_a.value()?
    );
    Ok(())
}
