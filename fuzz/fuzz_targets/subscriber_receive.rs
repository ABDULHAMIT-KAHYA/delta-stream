#![no_main]

use delta_stream::{DeltaState, Subscriber};
use libfuzzer_sys::fuzz_target;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct FuzzState {
    value: u64,
    label: String,
}

fuzz_target!(|data: &[u8]| {
    let mut subscriber = Subscriber::<FuzzState>::new();
    let before = subscriber.state().cloned();
    let result = subscriber.receive(data);
    if result.is_err() {
        assert_eq!(subscriber.state().cloned(), before);
    }
});
