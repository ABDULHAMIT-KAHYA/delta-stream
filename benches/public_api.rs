use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use delta_stream::{DeltaState, Packet, Publisher, Subscriber};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
struct BenchState {
    id: u64,
    x: i32,
    y: i32,
    health: u16,
    stable: String,
    changed: Vec<u64>,
}

fn make_state(size: usize, seed: u64) -> BenchState {
    BenchState {
        id: seed,
        x: seed as i32,
        y: 20,
        health: 100_u16.saturating_sub((seed % 80) as u16),
        stable: "stable-state-field".repeat(size),
        changed: (0..size as u64).map(|value| value ^ seed).collect(),
    }
}

fn public_api_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("public_api_encode_receive");
    for (name, size) in [("small", 2), ("medium", 128), ("large", 1024)] {
        group.bench_with_input(
            BenchmarkId::new("json_full_serialize", name),
            &size,
            |b, size| {
                let state = make_state(*size, 1);
                b.iter(|| black_box(serde_json::to_vec(black_box(&state)).unwrap()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("publisher_encode_snapshot", name),
            &size,
            |b, size| {
                b.iter(|| {
                    let mut publisher = Publisher::<BenchState>::new();
                    black_box(publisher.encode(black_box(&make_state(*size, 1))).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("publisher_encode_delta", name),
            &size,
            |b, size| {
                b.iter(|| {
                    let mut publisher = Publisher::<BenchState>::new();
                    let _ = publisher.encode(&make_state(*size, 1)).unwrap();
                    black_box(publisher.encode(black_box(&make_state(*size, 2))).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("subscriber_receive", name),
            &size,
            |b, size| {
                let mut publisher = Publisher::<BenchState>::new();
                let first = publisher.encode(&make_state(*size, 1)).unwrap();
                let second = publisher.encode(&make_state(*size, 2)).unwrap();
                b.iter(|| {
                    let mut subscriber = Subscriber::<BenchState>::new();
                    let _ = subscriber.receive(black_box(&first)).unwrap();
                    black_box(subscriber.receive(black_box(&second)).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("low_level_update_to_bytes", name),
            &size,
            |b, size| {
                b.iter(|| {
                    let mut publisher = Publisher::<BenchState>::new();
                    let packet = publisher.update(black_box(&make_state(*size, 1))).unwrap();
                    black_box(packet.to_bytes().unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("low_level_from_bytes_apply", name),
            &size,
            |b, size| {
                let mut publisher = Publisher::<BenchState>::new();
                let bytes = publisher.encode(&make_state(*size, 1)).unwrap();
                b.iter(|| {
                    let mut subscriber = Subscriber::<BenchState>::new();
                    let packet = Packet::from_bytes(black_box(&bytes)).unwrap();
                    black_box(subscriber.apply(packet).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("recovery_snapshot", name),
            &size,
            |b, size| {
                let mut publisher = Publisher::<BenchState>::new();
                let state = make_state(*size, 2);
                let _ = publisher.encode(&make_state(*size, 1)).unwrap();
                let _ = publisher.encode(&state).unwrap();
                b.iter(|| black_box(publisher.recovery_snapshot(black_box(&state)).unwrap()));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, public_api_benchmarks);
criterion_main!(benches);
