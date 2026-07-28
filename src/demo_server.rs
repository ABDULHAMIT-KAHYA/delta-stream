use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

use serde::Serialize;

use crate::{
    error::DeltaError,
    metrics::Metrics,
    state::AgentState,
    sync::{ApplyResult, Decoder, Encoder},
};

const DASHBOARD: &str = include_str!("../demo/index.html");

#[derive(Debug)]
struct DemoState {
    current: AgentState,
    encoder: Encoder,
    decoder: Decoder,
    metrics: Metrics,
    drop_next: bool,
}

#[derive(Serialize)]
struct TickResponse {
    state: AgentState,
    metrics: Metrics,
    full_bytes: usize,
    delta_bytes: usize,
    dropped: bool,
    desync_detected: bool,
    resynced: bool,
    sequence: u64,
}

pub fn serve(addr: &str) -> Result<(), DeltaError> {
    let listener = TcpListener::bind(addr)?;
    let shared = Arc::new(Mutex::new(fresh_demo()));

    println!("DeltaStream dashboard: http://{addr}");
    println!("Press Ctrl+C to stop.");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let shared = Arc::clone(&shared);
                std::thread::spawn(move || {
                    if let Err(err) = handle(stream, shared) {
                        eprintln!("request error: {err}");
                    }
                });
            }
            Err(err) => eprintln!("connection error: {err}"),
        }
    }
    Ok(())
}

fn fresh_demo() -> DemoState {
    DemoState {
        current: AgentState::demo(),
        encoder: Encoder::default(),
        decoder: Decoder::default(),
        metrics: Metrics::default(),
        drop_next: false,
    }
}

fn handle(mut stream: TcpStream, shared: Arc<Mutex<DemoState>>) -> Result<(), DeltaError> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf)?;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let first = req.lines().next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");

    match (method, path) {
        ("GET", "/") => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            DASHBOARD.as_bytes(),
        )?,
        ("GET", "/api/tick") => {
            let mut s = shared
                .lock()
                .map_err(|_| DeltaError::InvalidState("demo mutex poisoned"))?;
            let next = s.current.advance();
            let full_bytes = serde_json::to_vec(&next)?.len();
            let packet = s.encoder.encode(&next)?;
            let packet_bytes = packet.encode()?.len();
            let dropped = std::mem::take(&mut s.drop_next);
            let mut desync_detected = false;
            let mut resynced = false;
            let mut wire_bytes = packet_bytes;
            let mut sequence = packet.sequence;

            s.metrics.updates += 1;
            s.metrics.full_json_bytes += full_bytes as u64;
            s.metrics.wire_bytes += packet_bytes as u64;
            if packet.kind == crate::packet::PacketKind::Delta {
                s.metrics.delta_packets += 1;
            } else {
                s.metrics.snapshot_packets += 1;
            }
            if packet.is_compressed() {
                s.metrics.compressed_packets += 1;
            }

            if !dropped {
                match s.decoder.apply_packet(packet)? {
                    ApplyResult::Applied { .. } => {}
                    ApplyResult::Duplicate { .. } => {
                        s.metrics.duplicates += 1;
                    }
                    ApplyResult::NeedSnapshot { .. } => {
                        desync_detected = true;
                        s.metrics.resyncs += 1;
                        let snapshot = s.encoder.recovery_snapshot(&next)?;
                        let snapshot_bytes = snapshot.encode()?.len();
                        wire_bytes += snapshot_bytes;
                        s.metrics.wire_bytes += snapshot_bytes as u64;
                        sequence = snapshot.sequence;
                        match s.decoder.apply_packet(snapshot)? {
                            ApplyResult::Applied { .. } => resynced = true,
                            ApplyResult::Duplicate { .. } => resynced = true,
                            ApplyResult::NeedSnapshot { .. } => {
                                return Err(DeltaError::InvalidState(
                                    "snapshot failed to resync decoder",
                                ));
                            }
                        }
                    }
                }
            }

            s.current = next.clone();
            let payload = serde_json::to_vec(&TickResponse {
                state: next,
                metrics: s.metrics.clone(),
                full_bytes,
                delta_bytes: wire_bytes,
                dropped,
                desync_detected,
                resynced,
                sequence,
            })?;
            respond(&mut stream, "200 OK", "application/json", &payload)?;
        }
        ("POST", "/api/drop") => {
            shared
                .lock()
                .map_err(|_| DeltaError::InvalidState("demo mutex poisoned"))?
                .drop_next = true;
            respond(&mut stream, "204 No Content", "text/plain", b"")?;
        }
        ("POST", "/api/reset") => {
            let mut s = shared
                .lock()
                .map_err(|_| DeltaError::InvalidState("demo mutex poisoned"))?;
            *s = fresh_demo();
            respond(&mut stream, "204 No Content", "text/plain", b"")?;
        }
        _ => respond(&mut stream, "404 Not Found", "text/plain", b"not found")?,
    }
    Ok(())
}

fn respond(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<(), DeltaError> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}
