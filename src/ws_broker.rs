use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::broadcast};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use crate::error::DeltaError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RelayFrame {
    Publish { topic: String, payload_hex: String },
    Subscribe { topics: Vec<String> },
    Message { topic: String, payload_hex: String },
}

pub async fn run(addr: &str) -> Result<(), DeltaError> {
    let listener = TcpListener::bind(addr).await?;
    let (bus, _) = broadcast::channel::<(String, String)>(2048);
    println!("DeltaStream WebSocket relay listening on ws://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let bus = bus.clone();
        tokio::spawn(async move {
            let mut ws = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    eprintln!("websocket accept failed: {e}");
                    return;
                }
            };
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                return;
            };
            let frame = match serde_json::from_str::<RelayFrame>(&text) {
                Ok(frame) => frame,
                Err(e) => {
                    eprintln!("invalid relay frame: {e}");
                    return;
                }
            };
            match frame {
                RelayFrame::Publish { topic, payload_hex } => {
                    let _ = bus.send((topic, payload_hex));
                }
                RelayFrame::Subscribe { topics } => {
                    let mut rx = bus.subscribe();
                    loop {
                        match rx.recv().await {
                            Ok((topic, payload_hex)) if topics.iter().any(|t| t == &topic) => {
                                let frame = RelayFrame::Message { topic, payload_hex };
                                let text = match serde_json::to_string(&frame) {
                                    Ok(v) => v,
                                    Err(_) => break,
                                };
                                if ws.send(Message::Text(text)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(_) => {}
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(_) => break,
                        }
                    }
                }
                RelayFrame::Message { .. } => {}
            }
        });
    }
}
