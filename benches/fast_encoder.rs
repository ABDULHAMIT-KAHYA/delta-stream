use criterion::{black_box, criterion_group, criterion_main, Criterion};
use delta_stream::FastByteStateEncoder;

fn bench_v30(c: &mut Criterion) {
    c.bench_function("v30 fast encoder 100KiB 1pct", |b| {
        let mut enc = FastByteStateEncoder::new("bench/v30");
        let mut state = vec![0xA5u8; 100 * 1024];
        let mut update = 1usize;
        let _ = enc.encode(&state).unwrap();
        b.iter(|| {
            for n in 0..1024usize {
                let i = (update * 97 + n * 7919) % state.len();
                state[i] ^= (update as u8).wrapping_add(1);
            }
            update = update.wrapping_add(1);
            black_box(enc.encode(black_box(&state)).unwrap());
        });
    });
}
criterion_group!(benches, bench_v30);
criterion_main!(benches);
