# DeltaStream

<p align="center">
  <strong>Reliable application-state synchronization for Rust.</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/delta-stream">
    <img src="https://img.shields.io/crates/v/delta-stream.svg" alt="crates.io version">
  </a>
  <a href="https://docs.rs/delta-stream">
    <img src="https://docs.rs/delta-stream/badge.svg" alt="docs.rs">
  </a>
  <a href="https://github.com/ABDULHAMIT-KAHYA/delta-stream/actions/workflows/ci.yml">
    <img src="https://github.com/ABDULHAMIT-KAHYA/delta-stream/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License">
  </a>
</p>

DeltaStream turns serializable Rust state into validated **snapshot** or **delta** packets that can travel over any byte-capable transport.

```text
State â†’ Publisher â†’ bytes â†’ transport â†’ bytes â†’ Subscriber â†’ synchronized state
```

It works above TCP, WebSocket, PubNub, MQTT, NATS, IPC, files, or any custom transport.

> **Latest release:** `0.32.0`  
> **Status:** Public preview

## Why DeltaStream?

Sending a complete state object after every small update wastes bandwidth and makes recovery logic your problem.

DeltaStream provides:

- snapshot and delta generation;
- automatic snapshot-versus-delta selection;
- optional zstd compression;
- sequence and duplicate detection;
- state-hash validation;
- CRC32 packet integrity checks;
- explicit recovery after packet loss;
- bounded decoding for untrusted input;
- transport-independent serialized bytes.

## Installation

```toml
[dependencies]
delta-stream = "0.31.0"
serde = { version = "1", features = ["derive"] }
```

## Quick Start

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

    assert_eq!(subscriber.state(), Some(&state));

    println!("State synchronized.");
    Ok(())
}
```

The common path is intentionally small:

```rust
let bytes = publisher.encode(&state)?;
subscriber.receive(&bytes)?;
```

## Updates

The first call normally creates a snapshot. Later compatible changes may be encoded as smaller deltas.

```rust
let first = PlayerState {
    name: "Abdulhamit".into(),
    x: 10,
    y: 20,
    health: 100,
};

subscriber.receive(&publisher.encode(&first)?)?;

let updated = PlayerState {
    x: 11,
    health: 98,
    ..first
};

subscriber.receive(&publisher.encode(&updated)?)?;

assert_eq!(subscriber.state(), Some(&updated));
```

## Packet-Loss Recovery

When a required delta is missing, the subscriber returns `Apply::NeedSnapshot`.

```rust
use delta_stream::Apply;

match subscriber.receive(&bytes)? {
    Apply::Applied { sequence, .. } => {
        println!("Applied sequence {sequence}");
    }

    Apply::Duplicate { sequence } => {
        println!("Ignored duplicate sequence {sequence}");
    }

    Apply::NeedSnapshot {
        local_sequence,
        required_sequence,
    } => {
        println!(
            "Recovery required: local={local_sequence:?}, required={required_sequence}"
        );
    }
}
```

Create a recovery snapshot with:

```rust
let packet = publisher.recovery_snapshot(&current_state)?;
subscriber.apply(packet)?;
```

Recovery snapshots preserve the publisherâ€™s current sequence, so repairing one subscriber does not disrupt healthy subscribers.

## Lower-Level Packet API

Use the packet API when you need metadata inspection, persistence, custom buffering, or transport-specific control.

```rust
use delta_stream::Packet;

let packet = publisher.update(&state)?;
let bytes = packet.to_bytes()?;

let packet = Packet::from_bytes(&bytes)?;
let result = subscriber.apply(packet)?;
```

The high-level methods are equivalent to:

```text
Publisher::encode  = Publisher::update + Packet::to_bytes
Subscriber::receive = Packet::from_bytes + Subscriber::apply
```

## Decode Safety

`Packet::from_bytes` and `Subscriber::receive` use bounded defaults:

- maximum encoded packet size: `64 MiB + header`;
- maximum decompressed payload: `64 MiB`.

Use `DecodeConfig` for tighter deployment limits:

```rust
use delta_stream::{DecodeConfig, Subscriber};

let config = DecodeConfig {
    max_packet_bytes: 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

let subscriber = Subscriber::<PlayerState>::with_decode_config(config);
```

Malformed, truncated, corrupted, unsupported, or oversized packets return controlled errors instead of panicking.

A rejected packet does not partially mutate subscriber state.

## Compatibility

DeltaStream `0.31.0` writes wire version `3`.

The current decoder accepts:

```text
wire versions 2..=3
```

Schema compatibility is based on the `DeltaState` schema hash. Incompatible deltas are rejected, while snapshot migrations remain explicit through `MigrationRegistry`.

See [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).

## Optional CRDT Support

The `crdt` feature adds a separate multi-writer replicated-value layer. It does not change the authoritative `Publisher`/`Subscriber` synchronization API.

```toml
[dependencies]
delta-stream = { version = "0.31.0", features = ["crdt"] }
```

Current CRDT types are `GCounter`, `PNCounter`, and `LwwRegister`. Their state-based merge operations tolerate duplicate and reordered delivery after every replica eventually receives the same states. The transport still handles delivery, authentication, encryption, and persistence.

```rust
use delta_stream::crdt::{Crdt, GCounter, ReplicaId};

# fn main() -> Result<(), delta_stream::crdt::CrdtError> {
let a = ReplicaId::new("a")?;
let b = ReplicaId::new("b")?;
let mut left = GCounter::new();
let mut right = GCounter::new();

left.increment(&a, 2)?;
right.increment(&b, 3)?;

left.merge(&right);
right.merge(&left);

assert_eq!(left.value()?, 5);
# Ok(())
# }
```

`LwwRegister` timestamps are supplied by the application; use monotonic logical timestamps, Lamport clocks, or another suitable ordering source. This feature does not claim arbitrary Rust struct CRDT support or collaborative-document semantics. See [docs/CRDT.md](docs/CRDT.md).
## Feature Flags

| Feature | Purpose |
|---|---|
| `derive` | Re-export `#[derive(DeltaState)]`. Enabled by default. |
| `zstd-compression` | Enable adaptive zstd candidates. Enabled by default. |
| `crdt` | Enable optional state-based CRDT types. Disabled by default. |
| `pubnub-transport` | Enable the PubNub adapter. |
| `websocket-transport` | Enable the WebSocket adapter. |
| `mqtt-transport` | Enable the MQTT adapter. |
| `nats-transport` | Enable the NATS adapter. |
| `all-transports` | Enable every transport adapter. |
| `full` | Enable derive, compression, and all transports. |

Transport features remain optional, so the core crate does not require an async runtime unless a selected adapter needs one.

## Examples

```powershell
cargo run --example basic_sync
cargo run --example recovery
```

- `basic_sync` demonstrates snapshot and delta synchronization.
- `recovery` drops a packet, detects the gap, applies a recovery snapshot, and verifies convergence.

## Validation

```powershell
cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo bench --no-run
```

Run heavy ignored tests explicitly:

```powershell
cargo test --workspace --all-features -- --ignored --nocapture
```

## Fuzzing

The repository includes fuzz targets for packet decoding and subscriber receiving.

```powershell
rustup toolchain install nightly
cargo install cargo-fuzz

cargo +nightly fuzz run packet_from_bytes
cargo +nightly fuzz run subscriber_receive
```

Native Windows fuzzing may require the Visual Studio C++ AddressSanitizer runtime.

Both targets completed extended pre-release runs for `0.31.0` without crash artifacts or sanitizer failures. Fuzzing does not replace a formal security audit.

## Limitations

DeltaStream does not provide:

- network delivery guarantees;
- authentication or authorization;
- encryption;
- global persistence;
- cross-language protocol stability;
- automatic arbitrary schema migration;
- media compression.

High-entropy or completely unrelated state changes may be more efficient as snapshots.

Before `1.0`, minor releases may refine APIs and compatibility rules.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Protocol and recovery](docs/PROTOCOL.md)
- [Compatibility](docs/COMPATIBILITY.md)
- [Optional CRDT support](docs/CRDT.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Security](docs/SECURITY.md)
- [Release process](docs/RELEASE.md)
- [Changelog](CHANGELOG.md)

## License

Licensed under the [MIT License](LICENSE).
