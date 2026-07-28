use delta_stream::{GenericApplyResult, StatePublisher, StateSubscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, delta_stream::DeltaState)]
struct GameState {
    x: i32,
    y: i32,
    hp: u16,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut publisher = StatePublisher::<GameState>::default();
    let mut subscriber = StateSubscriber::<GameState>::default();

    let a = GameState {
        x: 10,
        y: 20,
        hp: 100,
    };
    let b = GameState {
        x: 11,
        y: 20,
        hp: 98,
    };

    let p1 = publisher.update(&a)?;
    let p2 = publisher.update(&b)?;

    let _ = subscriber.apply(p1)?;
    match subscriber.apply(p2)? {
        GenericApplyResult::Applied { sequence, state } => {
            println!("seq={sequence} state={state:?}");
        }
        other => println!("{other:?}"),
    }

    Ok(())
}
