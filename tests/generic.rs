use delta_stream::{DeltaState, GenericApplyResult, GenericDecoder, GenericEncoder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, DeltaState)]
struct GameState {
    x: i32,
    y: i32,
    hp: u16,
    weapon: String,
}

#[test]
fn derived_generic_state_roundtrips() {
    let a = GameState {
        x: 1,
        y: 2,
        hp: 100,
        weapon: "rifle".into(),
    };
    let b = GameState {
        x: 3,
        y: 2,
        hp: 98,
        weapon: "rifle".into(),
    };
    let mut enc = GenericEncoder::<GameState>::default();
    let mut dec = GenericDecoder::<GameState>::default();
    let p1 = enc.encode(&a).unwrap();
    let p2 = enc.encode(&b).unwrap();
    let _ = dec.apply_packet(p1).unwrap();
    match dec.apply_packet(p2).unwrap() {
        GenericApplyResult::Applied { state, .. } => assert_eq!(state, b),
        _ => panic!("expected applied generic delta"),
    }
}
