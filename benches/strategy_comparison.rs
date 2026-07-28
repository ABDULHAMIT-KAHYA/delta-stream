use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use delta_stream::{smart_delta, ByteStateEncoder};

fn state(size: usize) -> Vec<u8> {
    let mut x = 0x123456789abcdef0u64;
    (0..size)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn bench_smart_delta(c: &mut Criterion) {
    let mut group = c.benchmark_group("v25 smart delta");
    for size in [1024usize, 16 * 1024, 100 * 1024] {
        let previous = state(size);
        let mut current = previous.clone();
        for i in (0..size).step_by(100) {
            current[i] ^= 0x5a;
        }
        group.bench_with_input(
            BenchmarkId::new("candidate generation", size),
            &size,
            |b, _| {
                b.iter(|| {
                    smart_delta::encode_candidates(
                        black_box(&previous),
                        black_box(&current),
                        Default::default(),
                    )
                    .unwrap()
                })
            },
        );
        let candidates =
            smart_delta::encode_candidates(&previous, &current, Default::default()).unwrap();
        for candidate in candidates {
            let name = format!("apply {:?}", candidate.kind);
            group.bench_with_input(BenchmarkId::new(name, size), &size, |b, _| {
                b.iter(|| {
                    smart_delta::apply(black_box(&previous), black_box(&candidate.payload)).unwrap()
                })
            });
        }
    }
    group.finish();
}

fn bench_v25_encoder(c: &mut Criterion) {
    let previous = state(100 * 1024);
    let mut current = previous.clone();
    for i in (0..current.len()).step_by(100) {
        current[i] ^= 0x11;
    }
    c.bench_function("v25 100KiB 1pct encode", |b| {
        b.iter_batched(
            || {
                let mut enc = ByteStateEncoder::new("criterion/v25");
                let _ = enc.encode(&previous).unwrap();
                enc
            },
            |mut enc| enc.encode(black_box(&current)).unwrap(),
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_smart_delta, bench_v25_encoder);
criterion_main!(benches);
