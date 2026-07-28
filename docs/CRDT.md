# Optional CRDT Support

DeltaStream's existing `Publisher` and `Subscriber` API remains the authoritative ordered state-synchronization path. The optional `crdt` feature adds a separate state-based CRDT layer for multi-writer replicated values.

CRDT values define their own merge behavior. They can be serialized into bytes and carried by PubNub, WebSocket, MQTT, NATS, TCP, files, IPC, or another byte-capable transport. Transport delivery is still the application's responsibility.

## State-Based Merge Model

The `Crdt` trait exposes:

```rust
pub trait Crdt {
    fn merge(&mut self, other: &Self) -> bool;
}
```

The boolean reports whether local state changed. Implementations must satisfy:

- commutativity: merge order does not change the final state;
- associativity: grouping does not change the final state;
- idempotency: repeated delivery of the same state does not change the result.

## Supported Types

### GCounter

`GCounter` is a grow-only counter backed by deterministic `BTreeMap<ReplicaId, u64>` components.

```text
merged[replica] = max(local[replica], remote[replica])
```

Each replica component only grows. Increment and total-value overflow return controlled errors.

### PNCounter

`PNCounter` combines two `GCounter` values: one positive and one negative.

```text
value = sum(positive components) - sum(negative components)
```

Positive and negative sides merge independently.

### LwwRegister

`LwwRegister<T>` stores one value and application-supplied metadata:

```text
(timestamp, replica_id)
```

The greater tuple wins. Equal timestamps are resolved by deterministic `ReplicaId` ordering. DeltaStream does not treat timestamps as wall-clock-safe; applications should use monotonic logical timestamps, Lamport clocks, or another suitable ordering source.

## Authoritative Sync Versus CRDT Replication

Use `Publisher` and `Subscriber` when one authoritative writer owns ordered state updates and sequence gaps should trigger recovery.

Use CRDTs when multiple replicas may update independently while offline and later converge after receiving the same states in any order, including duplicates.

CRDT messages do not use DeltaStream's authoritative packet sequence-gap behavior.

## Example

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

assert_eq!(left, right);
assert_eq!(left.value()?, 5);
# Ok(())
# }
```

## Limitations

This feature does not provide arbitrary-struct CRDT support, collaborative text editing, OR-Set semantics, causal tombstone collection, transport delivery guarantees, authentication, encryption, or a security audit. OR-Set support is intentionally out of scope for this change because it requires observed-remove tags and tombstone or causal semantics.
