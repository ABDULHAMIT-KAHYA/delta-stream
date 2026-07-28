#![no_main]

use delta_stream::Packet;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = Packet::from_bytes(data);
});
