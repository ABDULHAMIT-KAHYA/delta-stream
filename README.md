# DeltaStream

**State-aware realtime synchronization for Rust.**

DeltaStream turns application state updates into snapshots and compact deltas, validates the state chain at the receiver, and refuses unsafe deltas when a packet gap or base-state mismatch is detected.

It is designed to sit **on top of** realtime transports. Optional adapters are included for PubNub, WebSocket, MQTT, and NATS.

> Version: **0.30.0** — production-candidate / public preview. The protocol and API are usable and heavily tested, but the project does not yet claim long-term production history or an independent security audit.

## Why DeltaStream?

A typical realtime application repeatedly sends complete state:

```text
state A ── full state ──►
state B ── full state ──►
state C ── full state ──►
```

DeltaStream establishes state once, then sends the useful change when that is cheaper:

```text
snapshot ───────────────►
delta ──────────────────►
delta ──────────────────►
```

When the chain breaks, DeltaStream detects it instead of blindly applying the next delta:

```text
seq 1 ✓  snapshot
seq 2 ✓  delta
seq 3 ✓  delta
seq 4 ✓  delta
seq 5 ✗  lost
seq 6 →  base mismatch → recovery required
                    ↓
              recovery snapshot
                    ↓
                sync restored
```

## Quick start

Add the crate:

```toml
[dependencies]
delta-stream = "0.30"
serde = { version = "1", features = ["derive"] }
```

Define a state model and synchronize it:

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

    let p1 = publisher.update(&GameState { x: 10, y: 20, hp: 100 })?;
    subscriber.apply(p1)?;

    let p2 = publisher.update(&GameState { x: 11, y: 20, hp: 98 })?;

    match subscriber.apply(p2)? {
        Apply::Applied { sequence, state } => {
            println!("seq={sequence} state={state:?}");
        }
        Apply::NeedSnapshot {
            local_sequence,
            required_sequence,
        } => {
            println!("recovery required: local={local_sequence:?} base={required_sequence}");
        }
        Apply::Duplicate { sequence } => {
            println!("duplicate seq={sequence}");
        }
    }

    Ok(())
}
```

For most applications, the public API is intentionally small:

```text
Publisher<T>
Subscriber<T>
Packet
Apply<T>
DeltaState
DeltaError
```

Advanced protocol/tuning types are grouped under `delta_stream::advanced`.

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

## Moving packets over a transport

DeltaStream is transport-independent at its core. Serialize a packet before sending it and decode it after receiving it:

```rust
let bytes = packet.to_bytes()?;
let packet = delta_stream::Packet::from_bytes(&bytes)?;
```

Optional transport features:

| Feature | Adapter |
|---|---|
| `pubnub-transport` | PubNub |
| `websocket-transport` | WebSocket |
| `mqtt-transport` | MQTT |
| `nats-transport` | NATS |
| `all-transports` | All adapters |
| `full` | All transports + derive + compression |

Example:

```toml
[dependencies]
delta-stream = { version = "0.30", features = ["pubnub-transport"] }
```

## Recovery model

DeltaStream supports several recovery building blocks:

- sequence/base-state validation;
- duplicate and stale packet suppression;
- bounded reordering;
- replay from retained history;
- authoritative recovery snapshots;
- partial chunk repair for large byte states;
- runtime backpressure decisions for lagging clients.

A recovery snapshot is built at the **current sequence** and does not consume a new shared sequence number. That prevents one client's recovery from creating a gap for healthy clients.

## V30 adaptive byte-state path

The advanced V30 byte-state encoder profiles a transition, creates a bounded shortlist, and chooses among snapshot/delta/compressed candidates. Available delta families include Sparse, Ranges, XOR, Splice, and Chunks.

```rust
use delta_stream::advanced::{ByteStateDecoder, FastByteStateEncoder};

let mut publisher = FastByteStateEncoder::new("game/state");
let mut subscriber = ByteStateDecoder::new("game/state");

let first = vec![0u8; 4096];
let mut second = first.clone();
second[2000] = 7;

subscriber.apply(publisher.encode(&first)?)?;
subscriber.apply(publisher.encode(&second)?)?;
# Ok::<(), delta_stream::DeltaError>(())
```

## Measured V30 results

The following are **synthetic workload measurements**, not universal performance claims:

| Test | Observed result |
|---|---:|
| 100 KiB state, 1% mutation, 100k updates | ~98.30% packet-byte reduction vs repeatedly sending full state |
| V30 fast encoder, 100 KiB / 1% Criterion run | ~306 µs/update midpoint |
| V25 equivalent Criterion run | ~1.11 ms/update midpoint |
| Partial repair, 1 MiB state / four changed 1 KiB chunks | 4 KiB repair payload, 99.61% smaller than full snapshot payload |
| V30 hard simulation | 2,000/2,000 clients converged after 50,000 updates |
| Recovery storm | 10,000/10,000 clients converged |

See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for methodology and caveats.

## Real PubNub recovery demo

V30 was also exercised through the PubNub adapter with an intentional dropped delta:

```text
seq=1..4 applied
seq=5 intentionally dropped
DESYNC local=4 requires base=5
automatic RESYNC_REQUEST
recovery SNAPSHOT at seq=7
SYNC RESTORED at seq=7
seq=8..20 applied
ACK through seq=20
```

This demonstrates application-layer state recovery over the real transport; it is not a claim that PubNub itself loses messages.

## Quality gates

The release candidate has been exercised with:

```text
cargo check --all-features
cargo fmt -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo run --release -- --validate-v30
```

See [`docs/RELEASE.md`](docs/RELEASE.md) for the publish checklist.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Protocol and recovery model](docs/PROTOCOL.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Security](docs/SECURITY.md)
- [Release checklist](docs/RELEASE.md)
- [Changelog](CHANGELOG.md)

## Scope

DeltaStream is strongest when application state changes frequently but only part of the state changes each update: game state, AI-agent telemetry, IoT/device state, dashboards, collaborative state, presence/session state, and similar workloads.

It is **not** intended to replace specialized audio/video codecs, and completely unrelated high-entropy blobs may offer little or no delta advantage. In those cases, a snapshot/raw path is the correct behavior.

## License

MIT. See [LICENSE](LICENSE).
