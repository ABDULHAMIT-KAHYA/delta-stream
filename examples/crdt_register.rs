use delta_stream::crdt::{Crdt, LwwRegister, ReplicaId};

fn main() -> Result<(), delta_stream::crdt::CrdtError> {
    let replica_a = ReplicaId::new("replica-a")?;
    let replica_b = ReplicaId::new("replica-b")?;

    let register_a = LwwRegister::new("from-a".to_string(), 7, replica_a);
    let register_b = LwwRegister::new("from-b".to_string(), 7, replica_b);

    let mut merged_ab = register_a.clone();
    merged_ab.merge(&register_b);

    let mut merged_ba = register_b.clone();
    merged_ba.merge(&register_a);

    assert_eq!(merged_ab, merged_ba);
    assert_eq!(merged_ab.value(), "from-b");

    println!(
        "register replicas converged on {:?} from {}",
        merged_ab.value(),
        merged_ab.replica()
    );
    Ok(())
}
