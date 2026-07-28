use delta_stream::{Apply, DeltaState, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct PlayerState {
    name: String,
    x: i32,
    y: i32,
    health: u16,
    inventory: String,
}

fn player(x: i32, health: u16) -> PlayerState {
    PlayerState {
        name: "Abdulhamit".into(),
        x,
        y: 20,
        health,
        inventory: "stable inventory field ".repeat(256),
    }
}

fn main() -> Result<(), delta_stream::DeltaError> {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();

    let initial = player(10, 100);
    let dropped = player(11, 99);
    let current = player(12, 98);

    let bytes = publisher.encode(&initial)?;
    subscriber.receive(&bytes)?;
    println!("initial snapshot applied");

    let dropped_bytes = publisher.encode(&dropped)?;
    println!(
        "intentionally dropped sequence 2 ({} bytes)",
        dropped_bytes.len()
    );

    let later = publisher.encode(&current)?;
    match subscriber.receive(&later)? {
        Apply::NeedSnapshot { .. } => println!("gap detected"),
        other => println!("unexpected result before recovery: {other:?}"),
    }

    let recovery = publisher.recovery_snapshot(&current)?;
    subscriber.apply(recovery)?;
    println!("recovery snapshot applied");

    assert_eq!(subscriber.state(), Some(&current));
    println!("states converged");
    Ok(())
}
