DeltaStream

State-aware realtime synchronization for Rust.

DeltaStream turns application state changes into snapshots and compact deltas, validates the state chain at the receiver, and safely requests recovery when an update cannot be applied.

It runs above realtime transports such as PubNub, WebSocket, MQTT, and NATS.

Application state
       │
       ▼
 snapshot / compact delta
       │
       ▼
 PubNub · WebSocket · MQTT · NATS
       │
       ▼
 validation · ordering · recovery
       │
       ▼
 synchronized remote state

Latest release: 0.30.1Status: public preview / production-candidate. The protocol and API are usable and extensively tested, but the project does not yet claim long-term production deployment history, independent security auditing, or formal verification.

Why DeltaStream?

Realtime transports move messages. They generally do not know whether one application-state update depends on another.

A typical application repeatedly sends its complete state:

state A ── full state ──►
state B ── full state ──►
state C ── full state ──►

DeltaStream establishes the state once, then sends the useful change when that representation is smaller:

snapshot ───────────────►
delta from snapshot ────►
delta from previous ────►

The important difference is not only packet size. DeltaStream tracks the state chain and refuses unsafe updates.

seq 1  snapshot                 applied
seq 2  delta based on seq 1     applied
seq 3  delta based on seq 2     applied
seq 4  delta based on seq 3     applied
seq 5  lost
seq 6  delta based on seq 5     rejected
                                   │
                                   ▼
                            recovery required
                                   │
                                   ▼
                            recovery snapshot
                                   │
                                   ▼
                              sync restored

This prevents a receiver from silently constructing invalid state after packet loss, reordering, duplication, corruption, or base-state mismatch.

Quick start

Add the crate:

[dependencies]
delta-stream = "0.30.1"
serde = { version = "1", features = ["derive"] }

Define a state model and synchronize it:

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

    let initial = GameState {
        x: 10,
        y: 20,
        hp: 100,
    };

    let updated = GameState {
        x: 11,
        y: 20,
        hp: 98,
    };

    subscriber.apply(publisher.update(&initial)?)?;

    match subscriber.apply(publisher.update(&updated)?)? {
        Apply::Applied { sequence, state } => {
            println!("seq={sequence} state={state:?}");
        }

        Apply::NeedSnapshot {
            local_sequence,
            required_sequence,
        } => {
            println!(
                "recovery required: local={local_sequence:?}, required={required_sequence}"
            );
        }

        Apply::Duplicate { sequence } => {
            println!("duplicate seq={sequence}");
        }
    }

    Ok(())
}

The primary public API is intentionally small:

Publisher<T>

Subscriber<T>

Packet

Apply<T>

DeltaState

DeltaError

Protocol internals and tuning types are available under delta_stream::advanced.

Correctness and recovery

DeltaStream provides application-layer synchronization primitives for:

sequence and base-state validation;

duplicate and stale-packet suppression;

bounded packet reordering;

replay from retained history;

authoritative recovery snapshots;

partial repair of large byte states;

backpressure decisions for lagging subscribers;

payload integrity validation.

A recovery snapshot represents the publisher's current sequence without consuming another shared sequence number. Recovering one subscriber therefore does not create a new sequence gap for healthy subscribers.

Transport independence

DeltaStream packets can be serialized and carried by any byte-capable transport:

let bytes = packet.to_bytes()?;
let decoded = delta_stream::Packet::from_bytes(&bytes)?;

Optional adapters are available through Cargo features:

Feature

Adapter

pubnub-transport

PubNub

websocket-transport

WebSocket

mqtt-transport

MQTT

nats-transport

NATS

all-transports

All transport adapters

full

All transports, derive support, and compression

Example:

[dependencies]
delta-stream = {
    version = "0.30.1",
    features = ["pubnub-transport"]
}

Custom publisher policy

use delta_stream::{DeltaState, Publisher};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, DeltaState)]
struct State {
    value: u64,
}

let publisher = Publisher::<State>::builder()
    .compression(true)
    .zstd_level(1)
    .build();

Adaptive byte-state synchronization

For ordinary serializable Rust models, Publisher<T> chooses between complete snapshots and compact updates.

For large byte states, the advanced encoder evaluates a bounded set of representations and selects the smallest suitable candidate. Available strategies include:

sparse changes;

changed ranges;

XOR deltas;

splice operations;

chunk deltas;

compressed snapshots;

raw snapshots.

use delta_stream::advanced::{ByteStateDecoder, FastByteStateEncoder};

let mut publisher = FastByteStateEncoder::new("game/state");
let mut subscriber = ByteStateDecoder::new("game/state");

let first = vec![0_u8; 4096];
let mut second = first.clone();
second[2000] = 7;

subscriber.apply(publisher.encode(&first)?)?;
subscriber.apply(publisher.encode(&second)?)?;

# Ok::<(), delta_stream::DeltaError>(())

Measured results

These are synthetic workload measurements, not universal performance claims.

Workload

Observed result

100 KiB state, 1% mutation, 100,000 updates

approximately 98.30% fewer packet bytes than repeatedly sending the complete state

100 KiB state, 1% mutation

approximately 306 µs per encode at the Criterion midpoint

1 MiB state, four changed 1 KiB chunks

4 KiB repair payload

2,000-client fault simulation

2,000 clients converged

10,000-client recovery storm

10,000 clients converged

Results depend on state shape, mutation pattern, compression settings, hardware, and transport overhead.

See docs/BENCHMARKS.md for hardware, methodology, commands, and caveats.

PubNub recovery demonstration

DeltaStream was exercised over the PubNub adapter with one intentionally discarded delta:

seq 1–4    applied
seq 5      intentionally discarded
seq 6      rejected because its required base was missing
           recovery requested
seq 7      recovery snapshot applied
seq 8–20   normal synchronization continued

This demonstrates application-layer recovery over a real transport. It is not a claim that PubNub itself loses messages.

Quality gates

The current release has been exercised with:

cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run --release -- --validate-v30

The full suite covers:

packet round-tripping and integrity checks;

snapshot and delta reconstruction;

duplicate and stale packet handling;

gap detection and recovery;

adaptive snapshot fallback;

bounded reordering;

property-based delta tests;

malformed packet rejection;

partial repair;

backpressure behavior;

multi-client convergence;

public API recovery behavior;

documentation examples.

See docs/RELEASE.md for the release checklist.

Suitable workloads

DeltaStream is strongest when state changes frequently while only part of the state changes on each update:

multiplayer and simulation state;

AI-agent progress and telemetry;

IoT and device state;

live dashboards;

collaborative state;

presence and session state;

robotics and digital twins.

It is not a replacement for specialized audio or video codecs. High-entropy or completely unrelated states may provide little delta advantage; in those cases, DeltaStream can fall back to a snapshot.

Documentation

Architecture

Protocol and recovery

Benchmarks

Security

Release process

Changelog

License

MIT. See LICENSE.