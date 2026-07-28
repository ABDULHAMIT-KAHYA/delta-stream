# DeltaStream Compatibility

This document describes the compatibility behavior of the current 0.31.0 development tree. It is based on the implementation in this repository, not on a long-term 1.0 stability promise.

## Crate API

DeltaStream is still pre-1.0. Patch releases are expected to preserve source compatibility for documented public APIs whenever possible. Minor releases may add APIs and may refine compatibility behavior before 1.0, with changes recorded in `CHANGELOG.md`.

The 0.31.0 high-level API is additive. Existing lower-level calls remain available:

```rust
let packet = publisher.update(&state)?;
let bytes = packet.to_bytes()?;
let packet = delta_stream::Packet::from_bytes(&bytes)?;
let result = subscriber.apply(packet)?;
```

## Wire Format

The current writer emits wire version `3`. The current reader accepts versions `2..=3` through the packet envelope decoder.

The 0.31.0 changes do not intentionally change the wire envelope. Packet decoding now also enforces configurable encoded-packet and decompressed-payload limits.

Older writers that emit supported v2 or v3 envelopes can be read when packet metadata, schema hash, compression flags, payload checksum, and payload serialization match what the current state type expects. Newer writers cannot assume older readers will understand future wire versions, flags, or payload conventions.

Unsupported wire versions return `DeltaError::ProtocolVersionRange`. Unknown packet kinds or flags return `DeltaError::InvalidPacket`.

## Schema Compatibility

Generic state synchronization uses `DeltaState::SCHEMA_NAME` to compute a schema hash. A packet whose schema hash differs from the subscriber state type is rejected with `DeltaError::SchemaMismatch` unless the advanced `apply_with_migrations` snapshot path is used.

Deltas require the subscriber to already hold the exact base sequence and base-state hash. If the sequence or base hash is missing or incompatible, the subscriber returns `Apply::NeedSnapshot` rather than applying an unsafe delta.

A fresh snapshot is required after packet loss, an incompatible base state, or a schema change that is not handled by an explicit snapshot migration. Recovery snapshots do not automatically bridge arbitrary schema changes.

## Recovery Semantics

`Publisher::recovery_snapshot` creates an authoritative snapshot at the publisher's current stream sequence and does not advance that sequence. This lets one lagging subscriber recover without creating a new sequence gap for healthy subscribers.

## What Is Guaranteed Today

- `Publisher::encode` is equivalent to `Publisher::update(...)?` followed by `Packet::to_bytes()`.
- `Subscriber::receive` is equivalent to packet decoding followed by `Subscriber::apply(...)`, using the subscriber's decode configuration.
- CRC mismatches and malformed packet envelopes are rejected before state application.
- Deltas are applied only when the base sequence and base-state hash match.
- Rejected packets do not partially mutate subscriber state.

## What Is Not Yet Guaranteed

- Cross-language protocol compatibility.
- Long-term wire-format stability across all future pre-1.0 minor releases.
- Automatic schema migration for deltas.
- Guaranteed delivery, ordering, authentication, or encryption at the transport layer.
- Formal security-audit coverage.
