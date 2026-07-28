use delta_stream::{DeltaState, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct PlayerState {
    name: String,
    x: i32,
    y: i32,
    health: u16,
}

fn main() -> Result<(), delta_stream::DeltaError> {
    let mut publisher = Publisher::<PlayerState>::new();
    let mut subscriber = Subscriber::<PlayerState>::new();

    let mut player = PlayerState {
        name: "Abdulhamit".into(),
        x: 10,
        y: 20,
        health: 100,
    };

    let bytes = publisher.encode(&player)?;
    subscriber.receive(&bytes)?;
    println!("initial state applied: {:?}", subscriber.state());

    player.x = 11;
    player.health = 98;

    let bytes = publisher.encode(&player)?;
    subscriber.receive(&bytes)?;
    println!("updated state applied: {:?}", subscriber.state());

    assert_eq!(subscriber.state(), Some(&player));
    Ok(())
}
