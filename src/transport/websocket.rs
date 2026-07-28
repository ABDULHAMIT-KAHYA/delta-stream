use async_trait::async_trait;
use futures::{stream::BoxStream, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    error::DeltaError,
    transport::{RealtimeTransport, TransportMessage},
};

#[derive(Debug, Clone)]
pub struct WebSocketRealtimeTransport {
    pub url: String,
}
impl WebSocketRealtimeTransport {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("DELTASTREAM_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:8790".into()),
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RelayFrame {
    Publish { topic: String, payload_hex: String },
    Subscribe { topics: Vec<String> },
    Message { topic: String, payload_hex: String },
}

#[async_trait]
impl RealtimeTransport for WebSocketRealtimeTransport {
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), DeltaError> {
        let (mut ws, _) = connect_async(self.url.as_str())
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        let frame = RelayFrame::Publish {
            topic: topic.into(),
            payload_hex: hex_encode(&payload),
        };
        ws.send(Message::Text(serde_json::to_string(&frame)?))
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        ws.close(None)
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(
        &self,
        topics: Vec<String>,
    ) -> Result<BoxStream<'static, Result<TransportMessage, DeltaError>>, DeltaError> {
        let (mut ws, _) = connect_async(self.url.as_str())
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        let frame = RelayFrame::Subscribe { topics };
        ws.send(Message::Text(serde_json::to_string(&frame)?))
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            while let Some(item) = ws.next().await {
                let result = match item {
                    Ok(Message::Text(text)) => serde_json::from_str::<RelayFrame>(&text)
                        .map_err(DeltaError::from)
                        .and_then(|frame| match frame {
                            RelayFrame::Message { topic, payload_hex } => hex_decode(&payload_hex)
                                .map(|payload| TransportMessage { topic, payload })
                                .map_err(DeltaError::Transport),
                            _ => Err(DeltaError::Transport(
                                "unexpected websocket relay frame".into(),
                            )),
                        }),
                    Ok(_) => continue,
                    Err(e) => Err(DeltaError::Transport(e.to_string())),
                };
                if tx.send(result).await.is_err() {
                    break;
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 15) as usize] as char);
    }
    out
}
fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("odd hex length".into());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((nib(pair[0])? << 4) | nib(pair[1])?))
        .collect()
}
fn nib(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err("invalid hex".into()),
    }
}
