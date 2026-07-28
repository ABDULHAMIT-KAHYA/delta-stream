use delta_stream::{Apply, DeltaState, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct State {
    counter: u64,
}

fn main() -> Result<(), delta_stream::DeltaError> {
    let mut publisher = Publisher::<State>::new();
    let mut subscriber = Subscriber::<State>::new();

    let s1 = State { counter: 1 };
    let s2 = State { counter: 2 };
    let s3 = State { counter: 3 };

    subscriber.apply(publisher.update(&s1)?)?;

    // Simulate a dropped update: the publisher advances to seq=2 but the
    // subscriber never receives it.
    let _dropped = publisher.update(&s2)?;
    let next = publisher.update(&s3)?;

    match subscriber.apply(next)? {
        Apply::NeedSnapshot { .. } => {
            let recovery = publisher.recovery_snapshot(&s3)?;
            subscriber.apply(recovery)?;
            println!("sync restored at seq={:?}", subscriber.sequence());
        }
        other => println!("unexpected result: {other:?}"),
    }

    assert_eq!(subscriber.state(), Some(&s3));
    Ok(())
}
