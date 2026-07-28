# DeltaStream

<p align="center">
  <strong>Application-layer state synchronization for Rust.</strong>
</p>

DeltaStream turns serializable application states into validated snapshot or delta packets that can travel over any byte-capable transport. It runs above transports such as TCP, WebSocket, PubNub, MQTT, NATS, IPC channels, files, or UDP when reliability and ordering are handled externally.

```text
Application state
    -> Publisher
    -> snapshot or delta packet
    -> serialized bytes
    -> any byte-capable transport
    -> received bytes
    -> Subscriber
    -> validated synchronized state
```

> **Development version:** `0.31.0`  
> **Status:** Public preview / production-candidate

The core protocol and recovery behavior are tested, but the project has not yet received a formal security audit or broad long-term production validation.

## Quick Start

```toml
[dependencies]
delta-stream = "0.31.0"
serde = { version = "1", features = ["derive"] }
```

```rust
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

    let state = PlayerState {
        name: "Abdulhamit".into(),
        x: 10,
        y: 20,
        health: 100,
    };

    let bytes = publisher.encode(&state)?;
    subscriber.receive(&bytes)?;

    println!("{:?}", subscriber.state());

    Ok(())
}
```

`Publisher::encode` means: create a snapshot or delta, then serialize the packet.

`Subscriber::receive` means: parse bytes, validate packet integrity, then apply the packet.

## Advanced Packet Control

The lower-level packet API remains available when you need to inspect metadata, persist packets, or integrate with custom buffering.

```rust
# use delta_stream::{DeltaState, Packet, Publisher, Subscriber};
# use serde::{Deserialize, Serialize};
# #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
# struct State { value: u64 }
# fn main() -> Result<(), delta_stream::DeltaError> {
# let state = State { value: 1 };
let mut publisher = Publisher::<State>::new();
let mut subscriber = Subscriber::<State>::new();

let packet = publisher.update(&state)?;
let bytes = packet.to_bytes()?;

let packet = Packet::from_bytes(&bytes)?;
let result = subscriber.apply(packet)?;
# let _ = result;
# Ok(())
# }
```

## Behavior

DeltaStream provides:

- snapshot generation for initial sync and recovery;
- delta generation for compatible state transitions;
- adaptive choice between snapshot and delta based on encoded size;
- optional zstd compression candidate selection through the default `zstd-compression` feature;
- sequence validation and duplicate/stale packet detection;
- base-state hash validation before deltas are applied;
- CRC32 payload integrity checks in the wire packet;
- explicit `Apply::NeedSnapshot` results when a gap or incompatible base is detected;
- recovery snapshots that preserve the publisher's current sequence;
- schema hashes through `DeltaState::SCHEMA_NAME`;
- explicit snapshot migrations through `MigrationRegistry` for advanced users.

A rejected packet does not partially mutate subscriber state.

## Decode Safety Limits

`Packet::from_bytes` and `Subscriber::receive` use safe default limits:

- maximum encoded packet size: `64 MiB + packet header`;
- maximum logical payload after decompression: `64 MiB`.

Use `DecodeConfig` and `Subscriber::with_decode_config` or `Packet::from_bytes_with_config` to choose tighter limits for a deployment. Oversized packets fail with `DeltaError::PacketTooLarge`; oversized logical payloads fail before unbounded allocation or during bounded decompression.

## Compatibility

The 0.31.0 public API is additive: `update`, `to_bytes`, `from_bytes`, and `apply` remain available. The wire envelope remains version `3`, and readers accept wire versions `2..=3` as implemented by the current packet decoder.

Schema compatibility is based on the `DeltaState` schema hash. A delta with an incompatible schema is rejected. Snapshot migrations are explicit and only run through `apply_with_migrations`; recovery snapshots do not automatically bridge arbitrary schema changes.

More detail is in [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).

## Feature Flags

| Feature | Purpose |
|---|---|
| `derive` | Re-export the `DeltaState` derive macro. Enabled by default. |
| `zstd-compression` | Allow adaptive zstd packet candidates. Enabled by default. |
| `pubnub-transport` | Optional PubNub adapter. |
| `websocket-transport` | Optional WebSocket adapter. |
| `mqtt-transport` | Optional MQTT adapter. |
| `nats-transport` | Optional NATS adapter. |
| `all-transports` | Enable all optional transport adapters. |
| `full` | Enable all transports, derive support, and compression. |

Transport features are optional so the core crate does not require an async runtime unless you ask for one.

## Examples

```text
cargo run --example basic_sync
cargo run --example recovery
```

`examples/basic_sync.rs` demonstrates the high-level `encode`/`receive` workflow. `examples/recovery.rs` intentionally drops a delta, observes `Apply::NeedSnapshot`, applies a recovery snapshot, and verifies convergence.

## Testing And Benchmarks

Useful local checks:

```text
cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo bench --no-run
```

Benchmarks use Criterion and report synthetic workload behavior only. Do not treat benchmark output as a universal performance guarantee; results depend on CPU, OS, Rust version, state shape, iteration count, build profile, and compression settings. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Fuzzing

Fuzz targets live under `fuzz/` and are outside the normal dependency graph.

```text
cargo install cargo-fuzz
cargo fuzz run packet_from_bytes
cargo fuzz run subscriber_receive
```

The targets check that arbitrary bytes do not panic packet decoding or subscriber receiving.

## Limitations

DeltaStream does not provide transport delivery guarantees, authentication, encryption, cross-language protocol stability, or a replacement for media codecs. High-entropy states or completely unrelated updates may fall back to snapshots. Before 1.0, minor releases may refine public APIs or compatibility rules, with changes documented in the changelog.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Protocol and recovery](docs/PROTOCOL.md)
- [Compatibility](docs/COMPATIBILITY.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Security](docs/SECURITY.md)
- [Release process](docs/RELEASE.md)
- [Changelog](CHANGELOG.md)

## License

DeltaStream is licensed under the MIT License. See [LICENSE](LICENSE).
