# Benchmarks

DeltaStream benchmark numbers are workload-specific. This document records the important V30 observations without presenting them as universal results.

## 100 KiB sparse-update workload

Configuration used by the V30 benchmark:

- state size: 102,400 bytes;
- updates: 100,000;
- mutation rate: approximately 1% per update;
- high-entropy state workload;
- adaptive V30 byte-state encoder.

Observed run:

```text
wire bytes       173,611,110
saving           98.30%
wall             42.403 s
updates/sec      2,358
avg candidates   3.03
final convergence PASS
```

The percentage compares DeltaStream packet bytes to repeatedly transmitting the complete logical state for this synthetic workload. It does not include every possible transport/network framing byte.

## V25 vs V30 selector cost

A same-repository Criterion run measured approximately:

```text
V25 full 100 KiB / 1% encode midpoint: 1.1134 ms
V30 fast 100 KiB / 1% encode midpoint: 0.3065 ms
```

That is approximately a 3.6× reduction in the measured encoder benchmark time. The end-to-end 100,000-update executable benchmark improved from roughly 86.5 s in V25 to roughly 42.4 s in V30, but the two forms of benchmark measure different amounts of surrounding work.

## Partial repair

A 1 MiB state with four changed 1 KiB chunks produced:

```text
state bytes      1,048,576
changed chunks   4
repair bytes     4,096
saving           99.61%
exact repair     PASS
```

`repair bytes` above is the logical repair payload and is not a complete network-framing measurement.

## Fault simulation

One V30 high-scale deterministic simulation used:

```text
clients          2,000
updates          50,000
drops            473,473
duplicates       141,417
corruptions      99,538
reorders         195,346
recoveries       666,275
late joins       1,979
converged         2,000 / 2,000
```

A separate resync-storm test converged 10,000/10,000 simulated clients.

These are deterministic simulations, not measurements from 2,000 or 10,000 independently deployed production clients.

## Reproduce locally

```powershell
cargo run --release -- --v30-benchmark 100000
cargo run --release -- --partial-repair
cargo run --release -- --v30-torture-max 2000 50000
cargo run --release -- --resync-storm 10000
cargo bench
```

Record CPU model, Rust version, OS, feature flags, and commit hash when publishing new benchmark numbers.
