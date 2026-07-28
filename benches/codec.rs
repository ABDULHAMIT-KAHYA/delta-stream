use criterion::{black_box, criterion_group, criterion_main, Criterion};
use delta_stream::{AgentState, Decoder, Encoder};

fn codec_benchmark(c: &mut Criterion) {
    c.bench_function("v10 delta encode advancing AgentState", |b| {
        let mut state = AgentState::demo();
        let mut encoder = Encoder::default();
        let _ = encoder.encode(&state).unwrap();
        b.iter(|| {
            state = state.advance();
            black_box(encoder.encode(black_box(&state)).unwrap().encode().unwrap());
        });
    });

    c.bench_function("v10 delta decode advancing AgentState", |b| {
        let mut state = AgentState::demo();
        let mut encoder = Encoder::default();
        let mut packets = Vec::new();
        for _ in 0..1000 {
            packets.push(encoder.encode(&state).unwrap());
            state = state.advance();
        }
        b.iter(|| {
            let mut decoder = Decoder::default();
            for packet in packets.iter().cloned() {
                black_box(decoder.apply_packet(packet).unwrap());
            }
        });
    });
}
criterion_group!(benches, codec_benchmark);
criterion_main!(benches);
