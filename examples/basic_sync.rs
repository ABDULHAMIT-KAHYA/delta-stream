use delta_stream::{Apply, DeltaState, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct GameState {
    x: i32,
    y: i32,
    hp: u16,
}

fn main() -> Result<(), delta_stream::DeltaError> {
    let mut publisher = Publisher::<GameState>::new();
    let mut subscriber = Subscriber::<GameState>::new();

    let states = [
        GameState {
            x: 10,
            y: 20,
            hp: 100,
        },
        GameState {
            x: 11,
            y: 20,
            hp: 98,
        },
        GameState {
            x: 12,
            y: 21,
            hp: 96,
        },
    ];

    for state in states {
        let packet = publisher.update(&state)?;
        let wire = packet.to_bytes()?;
        let received = delta_stream::Packet::from_bytes(&wire)?;

        match subscriber.apply(received)? {
            Apply::Applied { sequence, state } => {
                println!("applied seq={sequence}: {state:?}");
            }
            Apply::NeedSnapshot { .. } => println!("recovery required"),
            Apply::Duplicate { sequence } => println!("duplicate seq={sequence}"),
        }
    }

    Ok(())
}
