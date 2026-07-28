# DeltaStream

<p align="center">
  <strong>Application-layer state synchronization for Rust.</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/delta-stream">
    <img src="https://img.shields.io/crates/v/delta-stream.svg" alt="crates.io">
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

DeltaStream converts serializable application state into validated snapshot or delta packets that can travel over any byte-capable transport.

It runs above transports such as TCP, WebSocket, PubNub, MQTT, NATS, IPC channels, files, or UDP when delivery, ordering, and reliability are handled externally.

Application state
    -> Publisher
    -> snapshot or delta
    -> serialized bytes
    -> any byte-capable transport
    -> received bytes
    -> Subscriber
    -> validated synchronized state

Latest release: 0.31.0Status: Public preview

The core synchronization, recovery, decoding, and malformed-input behavior are tested. DeltaStream has not yet received a formal security audit or broad long-term production validation.

Quick Start

Add DeltaStream and Serde:

[dependencies]
delta-stream = "0.31.0"
serde = { version = "1", features = ["derive"] }

Define a serializable state, derive DeltaState, then use Publisher and Subscriber:

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

    println!("state synchronized");

    Ok(())
}

The common API is intentionally small:

let bytes = publisher.encode(&state)?;
subscriber.receive(&bytes)?;

Publisher::encode:

creates a snapshot or delta;

applies the configured encoding and compression policy;

serializes the packet into transport-independent bytes.

Subscriber::receive:

parses the received bytes;

validates packet integrity and limits;

checks sequence and base-state requirements;

applies the packet transactionally.

Updating State

The first update normally produces a snapshot. Compatible later updates may produce deltas.

let initial = PlayerState {
    name: "Abdulhamit".into(),
    x: 10,
    y: 20,
    health: 100,
};

subscriber.receive(&publisher.encode(&initial)?)?;

let updated = PlayerState {
    name: "Abdulhamit".into(),
    x: 11,
    y: 20,
    health: 98,
};

subscriber.receive(&publisher.encode(&updated)?)?;

assert_eq!(subscriber.state(), Some(&updated));

DeltaStream decides whether a snapshot, delta, compressed snapshot, or compressed delta is the most suitable candidate according to the active policy.

Advanced Packet Control

The lower-level packet API remains available when an application needs to inspect metadata, persist packets, implement custom buffering, or control transport behavior directly.

use delta_stream::{DeltaState, Packet, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct State {
    value: u64,
}

fn main() -> Result<(), delta_stream::DeltaError> {
    let state = State { value: 1 };

    let mut publisher = Publisher::<State>::new();
    let mut subscriber = Subscriber::<State>::new();

    let packet = publisher.update(&state)?;
    let bytes = packet.to_bytes()?;

    println!(
        "kind={:?}, sequence={}, bytes={}",
        packet.kind,
        packet.sequence,
        bytes.len()
    );

    let packet = Packet::from_bytes(&bytes)?;
    let result = subscriber.apply(packet)?;

    println!("{result:?}");

    Ok(())
}

The high-level API is equivalent to the normal low-level path:

Publisher::encode
    = Publisher::update
    + Packet::to_bytes

Subscriber::receive
    = Packet::from_bytes
    + Subscriber::apply

Recovery After Packet Loss

When a subscriber receives a delta whose required base packet is missing, it returns Apply::NeedSnapshot.

use delta_stream::{Apply, DeltaState, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct State {
    value: u64,
    stable_data: String,
}

fn main() -> Result<(), delta_stream::DeltaError> {
    let mut publisher = Publisher::<State>::new();
    let mut subscriber = Subscriber::<State>::new();

    let stable_data = "recovery-example-".repeat(128);

    let state_1 = State {
        value: 1,
        stable_data: stable_data.clone(),
    };

    let state_2 = State {
        value: 2,
        stable_data: stable_data.clone(),
    };

    let state_3 = State {
        value: 3,
        stable_data,
    };

    subscriber.receive(&publisher.encode(&state_1)?)?;

    let dropped = publisher.encode(&state_2)?;
    println!("intentionally dropped {} bytes", dropped.len());

    let after_gap = publisher.encode(&state_3)?;

    match subscriber.receive(&after_gap)? {
        Apply::NeedSnapshot {
            local_sequence,
            required_sequence,
        } => {
            println!(
                "gap detected: local={local_sequence:?}, required={required_sequence}"
            );
        }

        other => panic!("expected snapshot request, received {other:?}"),
    }

    let recovery = publisher.recovery_snapshot(&state_3)?;
    subscriber.apply(recovery)?;

    assert_eq!(subscriber.state(), Some(&state_3));

    println!("recovery snapshot applied");
    println!("states converged");

    Ok(())
}

Recovery snapshots preserve the publisher's current sequence so repairing one subscriber does not create a new sequence gap for healthy subscribers.

Apply Results

Subscriber::receive and Subscriber::apply return an Apply<T> result.

match subscriber.receive(&bytes)? {
    Apply::Applied { sequence, state } => {
        println!("applied sequence {sequence}");
        println!("{state:?}");
    }

    Apply::Duplicate { sequence } => {
        println!("duplicate packet {sequence}");
    }

    Apply::NeedSnapshot {
        local_sequence,
        required_sequence,
    } => {
        println!(
            "snapshot required: local={local_sequence:?}, required={required_sequence}"
        );
    }
}

A rejected packet does not partially mutate subscriber state.

Behavior

DeltaStream provides:

initial snapshots;

compact deltas for compatible state transitions;

adaptive selection between snapshot and delta candidates;

optional zstd compression candidate selection;

sequence validation;

duplicate and stale packet suppression;

base-state hash validation before applying deltas;

CRC32 payload integrity checks;

explicit gap detection;

recovery snapshots;

schema hashes through DeltaState::SCHEMA_NAME;

explicit snapshot migrations through MigrationRegistry;

bounded packet decoding;

transactional subscriber updates;

optional reordering, partial repair, and backpressure-oriented components.

DeltaStream does not provide network delivery by itself.

The transport remains responsible for concerns such as:

establishing connections;

routing messages;

authentication and authorization;

encryption;

persistence;

access control;

retransmission;

delivery guarantees;

ordering guarantees where required.

Decode Safety Limits

Network input should be treated as untrusted.

Packet::from_bytes and Subscriber::receive use bounded default decoding limits:

maximum encoded packet size: 64 MiB + packet header;

maximum logical payload after decompression: 64 MiB.

Use DecodeConfig to apply tighter limits for a deployment.

use delta_stream::{DecodeConfig, Subscriber};

let config = DecodeConfig {
    max_packet_bytes: 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

let subscriber = Subscriber::<MyState>::with_decode_config(config);

Packets can also be decoded directly with a custom configuration:

use delta_stream::{DecodeConfig, Packet};

let config = DecodeConfig {
    max_packet_bytes: 1024 * 1024,
    max_decompressed_bytes: 8 * 1024 * 1024,
};

let packet = Packet::from_bytes_with_config(&bytes, &config)?;

Oversized encoded packets return DeltaError::PacketTooLarge.

Oversized logical payloads are rejected through bounded decoding and decompression behavior.

Malformed, truncated, corrupted, or unsupported packets return controlled errors rather than panicking.

Compatibility

The 0.31.0 public API is additive.

The existing lower-level methods remain available:

publisher.update(&state)?;
packet.to_bytes()?;
Packet::from_bytes(&bytes)?;
subscriber.apply(packet)?;

The wire envelope written by 0.31.0 remains version 3.

The current reader accepts wire versions:

2..=3

Compatibility still depends on:

supported wire version;

matching packet flags and payload representation;

valid CRC32 integrity checks;

compatible schema identity;

a valid delta base state.

Schema compatibility is based on the DeltaState schema hash.

A delta with an incompatible schema is rejected.

Snapshot migrations are explicit and run through apply_with_migrations. Recovery snapshots do not automatically bridge arbitrary schema changes.

See docs/COMPATIBILITY.md for details.

Feature Flags

Feature

Purpose

derive

Re-export the DeltaState derive macro. Enabled by default.

zstd-compression

Enable adaptive zstd packet candidates. Enabled by default.

pubnub-transport

Enable the optional PubNub adapter.

websocket-transport

Enable the optional WebSocket adapter.

mqtt-transport

Enable the optional MQTT adapter.

nats-transport

Enable the optional NATS adapter.

all-transports

Enable all optional transport adapters.

full

Enable transport adapters, derive support, and compression.

Transport dependencies remain optional so the core crate does not require an async runtime unless a transport feature needs one.

Examples

Run the basic synchronization example:

cargo run --example basic_sync

Run the recovery example:

cargo run --example recovery

examples/basic_sync.rs demonstrates:

deriving DeltaState;

encoding an initial state;

receiving and applying it;

encoding an update;

verifying final convergence.

examples/recovery.rs demonstrates:

applying an initial snapshot;

intentionally dropping a delta;

detecting the sequence gap;

receiving Apply::NeedSnapshot;

applying a recovery snapshot;

verifying convergence.

Testing

Run the main quality checks:

cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

Build documentation with warnings denied:

PowerShell

$env:RUSTDOCFLAGS = "-D warnings"
cargo doc --workspace --all-features --no-deps

Bash

RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

Run heavy ignored release tests explicitly:

cargo test --workspace --all-features -- --ignored --nocapture

Fuzzing

Fuzz targets live under fuzz/ and remain outside the normal dependency graph.

Install nightly Rust and cargo-fuzz:

rustup toolchain install nightly
cargo install cargo-fuzz

Run the packet parser target:

cargo +nightly fuzz run packet_from_bytes

Run the subscriber receive target:

cargo +nightly fuzz run subscriber_receive

On native Windows, Visual Studio C++ AddressSanitizer support may be required.

A typical Windows invocation is:

cargo +nightly fuzz run packet_from_bytes --strip-dead-code
cargo +nightly fuzz run subscriber_receive --strip-dead-code

Before the 0.31.0 release, both fuzz targets completed extended smoke runs without producing crash artifacts or sanitizer failures.

Fuzzing increases confidence but does not replace a formal security audit.

Benchmarks

Compile all benchmarks:

cargo bench --no-run

Run the public API benchmark:

cargo bench --bench public_api

The benchmark suite includes coverage for:

full JSON serialization;

Publisher::encode;

Subscriber::receive;

lower-level packet encoding and decoding;

snapshot paths;

delta paths;

recovery snapshot creation and application;

compressed and uncompressed behavior where supported.

Benchmark output represents synthetic workloads only.

Results depend on:

CPU;

operating system;

Rust version;

build profile;

state shape;

changed-field distribution;

compression settings;

iteration count.

Do not treat benchmark results as universal performance guarantees.

See docs/BENCHMARKS.md.

Package Validation

Validate the derive crate:

cargo package -p delta-stream-derive

Validate the main crate after the matching derive version is available from crates.io:

cargo package -p delta-stream

The derive crate must be published before the main crate when both versions change.

Limitations

DeltaStream does not currently provide:

transport delivery guarantees;

authentication;

authorization;

encryption;

global persistence;

cross-language protocol stability;

formal verification;

a completed security audit;

media or video compression;

automatic migration across arbitrary schema changes.

High-entropy states or completely unrelated updates may be more efficient as snapshots than deltas.

Before 1.0, minor releases may refine APIs and compatibility rules. Changes are documented in the changelog.

Maturity

DeltaStream is currently a public-preview Rust library.

The project includes:

public API integration tests;

malformed-input tests;

compatibility tests;

property-style tests;

recovery and fault simulations;

heavy multi-client tests;

fuzz targets;

benchmark coverage;

external crates.io consumer validation.

The project has not yet accumulated broad long-term production usage or received a formal independent security audit.

Documentation

Architecture

Protocol and recovery

Compatibility

Benchmarks

Security

Release process

Changelog

Installation

The current crates.io release is:

[dependencies]
delta-stream = "0.31.0"

The derive macro is enabled by default through the derive feature.

For manual control:

[dependencies]
delta-stream = {
    version = "0.31.0",
    default-features = false,
    features = ["derive", "zstd-compression"]
}

License

DeltaStream is licensed under the MIT License.

See LICENSE.