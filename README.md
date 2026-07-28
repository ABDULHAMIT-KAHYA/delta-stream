# DeltaStream

<p align="center">
  <strong>State-aware realtime synchronization for Rust.</strong>
</p>

<p align="center">
  Snapshots when necessary. Compact deltas when useful. Safe recovery when the state chain breaks.
</p>

<p align="center">
  <a href="https://crates.io/crates/delta-stream">
    <img src="https://img.shields.io/crates/v/delta-stream.svg" alt="crates.io">
  </a>
  <a href="https://docs.rs/delta-stream">
    <img src="https://docs.rs/delta-stream/badge.svg" alt="docs.rs">
  </a>
  <a href="https://github.com/ABDULHAMIT-KAHYA/delta-stream/actions">
    <img src="https://github.com/ABDULHAMIT-KAHYA/delta-stream/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License">
  </a>
</p>

---

DeltaStream converts application state updates into snapshots and compact deltas, validates the state chain at the receiver, and refuses unsafe updates when a required base state is missing.

It is designed to sit above realtime transports such as PubNub, WebSocket, MQTT, and NATS.

```text
Application state
       |
       v
snapshot or compact delta
       |
       v
PubNub / WebSocket / MQTT / NATS
       |
       v
validation / ordering / recovery
       |
       v
synchronized remote state
```

> **Latest release:** `0.30.1`  
> **Status:** Public preview / production-candidate

DeltaStream is usable and extensively tested, but it does not yet claim long-term production deployment history, independent security auditing, or formal protocol verification.

## Why DeltaStream?

Realtime transports move messages.

They usually do not know whether one application-state update depends on another.

A typical application repeatedly sends its complete state:

```text
State A  -->  full state
State B  -->  full state
State C  -->  full state
```

DeltaStream establishes the state once, then sends the useful change when that representation is smaller:

```text
State A  -->  snapshot
State B  -->  compact delta
State C  -->  compact delta
```

The important difference is not only packet size.

DeltaStream tracks the state chain and refuses to apply an update when its required base state is unavailable.

```text
Sequence 1  snapshot             applied
Sequence 2  delta from 1         applied
Sequence 3  delta from 2         applied
Sequence 4  delta from 3         applied
Sequence 5  lost
Sequence 6  delta requiring 5    rejected
                                      |
                                      v
                              recovery required
                                      |
                                      v
                              recovery snapshot
                                      |
                                      v
                                sync restored
```

This prevents the receiver from silently constructing invalid state after packet loss, reordering, duplication, corruption, or base-state mismatch.

## Quick start

Add DeltaStream and Serde:

```toml
[dependencies]
delta-stream = "0.30.1"
serde = { version = "1", features = ["derive"] }
```

Define a state model:

```rust
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
            println!("sequence={sequence}, state={state:?}");
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
            println!("duplicate sequence={sequence}");
        }
    }

    Ok(())
}
```

## Small public API

The primary API is intentionally compact:

```text
Publisher<T>
Subscriber<T>
Packet
Apply<T>
DeltaState
DeltaError
```

Advanced protocol and tuning types are available under:

```rust
delta_stream::advanced
```

## What DeltaStream provides

DeltaStream includes application-layer synchronization support for:

- snapshot and delta generation;
- sequence validation;
- base-state validation;
- duplicate suppression;
- stale-packet suppression;
- bounded packet reordering;
- replay from retained history;
- authoritative recovery snapshots;
- partial repair for large byte states;
- backpressure decisions for lagging subscribers;
- CRC32 payload integrity validation.

## Recovery without breaking healthy clients

A recovery snapshot is created at the publisher's current sequence without consuming another shared sequence number.

That means recovering one subscriber does not create a new sequence gap for subscribers that are already healthy.

```text
Healthy client       seq 7 --> seq 8 --> seq 9

Lagging client        seq 4 --> recovery at seq 7 --> seq 8 --> seq 9
```

## Transport-independent packets

DeltaStream is transport-independent at its core.

Encode a packet before sending it:

```rust
let bytes = packet.to_bytes()?;
```

Decode it after receiving it:

```rust
let packet = delta_stream::Packet::from_bytes(&bytes)?;
```

Any byte-capable transport can carry the packet.

## Optional transport adapters

| Feature | Adapter |
|---|---|
| `pubnub-transport` | PubNub |
| `websocket-transport` | WebSocket |
| `mqtt-transport` | MQTT |
| `nats-transport` | NATS |
| `all-transports` | All transport adapters |
| `full` | All transports, derive support, and compression |

Example:

```toml
[dependencies]
delta-stream = {
    version = "0.30.1",
    features = ["pubnub-transport"]
}
```

## Custom publisher policy

```rust
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
```

## Adaptive byte-state synchronization

For normal serializable Rust models, `Publisher<T>` chooses between a complete snapshot and a compact update.

For large byte states, the advanced encoder evaluates a bounded set of candidate representations.

Available strategies include:

- sparse changes;
- changed ranges;
- XOR deltas;
- splice operations;
- chunk deltas;
- compressed snapshots;
- raw snapshots.

```rust
use delta_stream::advanced::{ByteStateDecoder, FastByteStateEncoder};

let mut publisher = FastByteStateEncoder::new("game/state");
let mut subscriber = ByteStateDecoder::new("game/state");

let first = vec![0_u8; 4096];

let mut second = first.clone();
second[2000] = 7;

subscriber.apply(publisher.encode(&first)?)?;
subscriber.apply(publisher.encode(&second)?)?;

# Ok::<(), delta_stream::DeltaError>(())
```

## Measured results

The following are synthetic workload measurements, not universal performance claims.

| Workload | Observed result |
|---|---:|
| 100 KiB state, 1% mutation, 100,000 updates | Approximately 98.30% fewer packet bytes than repeatedly sending full state |
| 100 KiB state, 1% mutation | Approximately 306 microseconds per encode |
| 1 MiB state, four changed 1 KiB chunks | 4 KiB repair payload |
| Multi-client fault simulation | 2,000 of 2,000 clients converged |
| Recovery storm simulation | 10,000 of 10,000 clients converged |

Performance depends on:

- state shape;
- mutation pattern;
- compression settings;
- hardware;
- compiler version;
- transport overhead.

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for methodology, commands, and caveats.

## PubNub recovery demonstration

DeltaStream was exercised over the PubNub adapter with one intentionally discarded delta:

```text
Sequences 1-4    applied normally
Sequence 5       intentionally discarded
Sequence 6       rejected because its base was missing
Recovery         requested
Sequence 7       recovery snapshot applied
Sequences 8-20   normal synchronization continued
```

This demonstrates DeltaStream's application-layer recovery over a real transport.

It is not a claim that PubNub itself loses messages.

## Quality gates

The current release passes:

```text
cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run --release -- --validate-v30
```

The test suite covers:

- packet encoding and decoding;
- CRC validation;
- exact state reconstruction;
- gap detection;
- snapshot recovery;
- duplicate suppression;
- stale-packet handling;
- bounded reordering;
- malformed packet rejection;
- property-based delta testing;
- partial repair;
- backpressure behavior;
- multi-client convergence;
- public API behavior;
- documentation examples.

## Suitable workloads

DeltaStream is strongest when state changes frequently while only part of it changes per update.

Examples include:

- multiplayer game state;
- distributed simulations;
- AI-agent telemetry;
- IoT device state;
- realtime dashboards;
- collaborative applications;
- presence and session state;
- robotics;
- digital twins.

## Scope

DeltaStream is not a replacement for specialized audio or video codecs.

High-entropy or completely unrelated states may provide little delta advantage. In those cases, DeltaStream can fall back to a snapshot.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Protocol and recovery](docs/PROTOCOL.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Security](docs/SECURITY.md)
- [Release process](docs/RELEASE.md)
- [Changelog](CHANGELOG.md)

## Installation

```toml
[dependencies]
delta-stream = "0.30.1"
```

## License

DeltaStream is licensed under the MIT License.

See [LICENSE](LICENSE).