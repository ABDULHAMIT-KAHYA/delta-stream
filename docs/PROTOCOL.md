# DeltaStream protocol notes (V30)

DeltaStream remains wire-protocol v3 compatible with V20/V25. V30 changes selection, recovery planning, and runtime policy rather than silently changing the packet envelope.

## State-chain rule

A delta is applied only when the receiver's sequence and base hash match the delta's declared base. Otherwise the receiver requests recovery. A snapshot is self-contained and can establish or restore state.

## V30 representation policy

The V30 fast encoder classifies a state transition with one cheap scan and shortlists at most two delta families before optional compression. This bounds strategy-search work instead of brute-forcing every representation on every update.

Supported smart delta families remain Sparse, Ranges, XOR, Splice, and Chunks. V30 chooses among a shortlist based on change ratio, run count, resize detection, and recent winners.

## Recovery

Recovery can choose among:

1. replaying a contiguous retained delta chain;
2. partial chunk repair when the receiver already has most of the authoritative state;
3. a full snapshot.

The planner compares estimated payload bytes and preserves exact-state hash validation after partial repair.

## Compatibility

V30 keeps the v3 wire envelope and retains the V25/V20 regression suites. This is intentional: adaptive policy can evolve without forcing a wire-format break.
