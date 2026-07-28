#![cfg(feature = "crdt")]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use delta_stream::crdt::{Crdt, GCounter, LwwRegister, PNCounter, ReplicaId};

fn id(index: usize) -> ReplicaId {
    ReplicaId::new(format!("replica-{index:03}")).unwrap()
}

fn gcounter_with_replicas(count: usize) -> GCounter {
    let mut counter = GCounter::new();
    for index in 0..count {
        counter.increment(&id(index), index as u64 + 1).unwrap();
    }
    counter
}

fn pncounter_with_replicas(count: usize) -> PNCounter {
    let mut counter = PNCounter::new();
    for index in 0..count {
        let replica = id(index);
        counter.increment(&replica, index as u64 + 1).unwrap();
        counter.decrement(&replica, (index % 3) as u64).unwrap();
    }
    counter
}

fn crdt_benchmarks(c: &mut Criterion) {
    c.bench_function("crdt/gcounter_increment", |b| {
        let replica = id(1);
        b.iter(|| {
            let mut counter = GCounter::new();
            counter.increment(black_box(&replica), 1).unwrap();
            black_box(counter);
        });
    });

    let mut group = c.benchmark_group("crdt/merge");
    for count in [3_usize, 16, 64, 256] {
        group.bench_with_input(BenchmarkId::new("gcounter", count), &count, |b, count| {
            let remote = gcounter_with_replicas(*count);
            b.iter(|| {
                let mut local = GCounter::new();
                black_box(local.merge(black_box(&remote)));
            });
        });

        group.bench_with_input(BenchmarkId::new("pncounter", count), &count, |b, count| {
            let remote = pncounter_with_replicas(*count);
            b.iter(|| {
                let mut local = PNCounter::new();
                black_box(local.merge(black_box(&remote)));
            });
        });
    }
    group.finish();

    c.bench_function("crdt/lww_register_merge", |b| {
        let remote = LwwRegister::new("remote".to_string(), 42, id(2));
        b.iter(|| {
            let mut local = LwwRegister::new("local".to_string(), 41, id(1));
            black_box(local.merge(black_box(&remote)));
        });
    });
}

criterion_group!(benches, crdt_benchmarks);
criterion_main!(benches);
