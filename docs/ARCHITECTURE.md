# DeltaStream V25 architecture

## Layers

1. **State model** — typed `DeltaState` models or byte-state synchronization.
2. **Delta strategy layer** — sparse, ranges, XOR, splice, and chunks.
3. **Adaptive codec layer** — raw snapshot/delta plus optional zstd candidates and a small self-tuning compression threshold.
4. **Wire protocol** — v3 envelope with sequence/base sequence, schema hash, CRC32, flags, and bounded payload sizes.
5. **Receiver safety** — base-sequence/base-hash validation, duplicate/stale suppression, bounded reorder buffering.
6. **Recovery** — replay missing contiguous history when cheaper than a snapshot; otherwise use an authoritative current snapshot.
7. **Transport** — PubNub, WebSocket, MQTT, NATS, or local adapters.

## Smart delta formats

All V25 smart-delta payloads begin with a one-byte strategy tag and a varint logical new length. Strategy-specific data follows.

### Sparse

Stores changed indexes as delta-varints plus one replacement byte.

Best fit: scattered small edits.

### Ranges

Stores changed contiguous regions as `(gap, length, bytes)` tuples.

Best fit: runs of adjacent changed bytes.

### XOR

Stores the bytewise XOR of equal-length states.

Best fit: XOR output with strong compressibility. Raw XOR is rarely the winner on its own; zstd(XOR) can be.

### Splice

Stores common prefix length, common suffix length, and replacement middle bytes.

Best fit: insertions, deletions, string/blob edits, growth and shrinkage.

### Chunks

Divides state into fixed-size chunks and stores only changed chunks.

Best fit: larger states with localized block changes.

## Cost selection

For byte-state synchronization V25 evaluates a family of candidates:

```text
snapshot
zstd(snapshot)
sparse
zstd(sparse)
ranges
zstd(ranges)
xor
zstd(xor)
splice
zstd(splice)
chunks
zstd(chunks)
```

The smallest safe score wins. A configurable compression CPU penalty can bias the selection away from zstd when byte savings are too small to justify additional CPU/latency.

`AdaptiveTuner` raises or lowers the minimum zstd payload threshold based on whether recent compression attempts actually win.

## Recovery history

`RecoveryHistory` retains a bounded number of recently published packets under both packet-count and byte limits.

For a subscriber at sequence `L` and publisher snapshot sequence `P`:

1. Find packets `L+1..P`.
2. Require a contiguous valid delta chain.
3. Sum encoded replay bytes.
4. Replay only if the chain is complete and replay bytes are lower than snapshot bytes.
5. Otherwise send the snapshot.

This bounds memory while reducing unnecessary full-state recovery for small gaps.

## Reorder buffer

`ReorderDecoder` holds a bounded future packet only when the gap is within the configured sequence window and the pending map is below its maximum size. A large gap becomes `NeedRecovery` immediately.

## Resync storm behavior

The V25 hard simulation creates one authoritative recovery snapshot for a global outage and clones/reuses that snapshot across affected clients. This models snapshot coalescing and avoids repeated state serialization/compression for identical recovery state.

## Resource limits

The wire envelope continues to enforce:

- maximum wire payload: 64 MiB
- maximum decompressed logical packet payload: 64 MiB
- unknown flag rejection
- CRC32 verification before application
- explicit schema checks

## Compatibility

The V25 library keeps the v3 wire envelope and continues accepting the compatible v2 envelope. V25's smart-delta format is a payload-level feature used by the byte-state API; existing V20 typed packets remain valid under the same envelope semantics.

## V30 fast adaptive layer

V30 adds a fast path beside the V25 exhaustive byte-state encoder. `ChangeProfile` scans a transition once, `StrategyAdvisor` shortlists likely delta families and learns recent winners, and `FastByteStateEncoder` evaluates only that bounded shortlist. The wire packet remains protocol v3.

`PartialRepair` adds chunk-hash based repair for receivers that already hold most of the authoritative state. `recovery_v30` compares retained replay, partial repair, and snapshot by estimated payload cost. `runtime` defines bounded operational policy and observability counters.
