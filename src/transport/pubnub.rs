use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use pubnub::{
    dx::*,
    subscribe::{EventEmitter, EventSubscriber, Subscriber},
};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    error::DeltaError,
    transport::{RealtimeTransport, TransportMessage},
};

#[derive(Debug, Clone)]
pub struct PubNubConfig {
    pub publish_key: String,
    pub subscribe_key: String,
    pub user_id: String,
}

impl PubNubConfig {
    pub fn from_env() -> Result<Self, DeltaError> {
        Ok(Self {
            publish_key: std::env::var("PUBNUB_PUBLISH_KEY")
                .map_err(|_| DeltaError::InvalidState("PUBNUB_PUBLISH_KEY missing"))?,
            subscribe_key: std::env::var("PUBNUB_SUBSCRIBE_KEY")
                .map_err(|_| DeltaError::InvalidState("PUBNUB_SUBSCRIBE_KEY missing"))?,
            user_id: std::env::var("PUBNUB_USER_ID").unwrap_or_else(|_| "delta-stream-rust".into()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PubNubRealtimeTransport {
    config: PubNubConfig,
}

impl PubNubRealtimeTransport {
    pub fn new(config: PubNubConfig) -> Result<Self, DeltaError> {
        if config.publish_key.is_empty() || config.subscribe_key.is_empty() {
            return Err(DeltaError::InvalidState("PubNub keys cannot be empty"));
        }
        Ok(Self { config })
    }
}

#[async_trait]
impl RealtimeTransport for PubNubRealtimeTransport {
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), DeltaError> {
        let client = PubNubClientBuilder::with_reqwest_transport()
            .with_keyset(Keyset {
                subscribe_key: self.config.subscribe_key.as_str(),
                publish_key: Some(self.config.publish_key.as_str()),
                secret_key: None,
            })
            .with_user_id(self.config.user_id.as_str())
            .build()
            .map_err(|e| DeltaError::Transport(e.to_string()))?;

        let message = json!({
            "protocol": "delta-stream/transport/1",
            "encoding": "hex",
            "payload": hex_encode(&payload),
        });

        client
            .publish_message(message)
            .channel(topic)
            .execute()
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(
        &self,
        topics: Vec<String>,
    ) -> Result<BoxStream<'static, Result<TransportMessage, DeltaError>>, DeltaError> {
        let (tx, rx) = mpsc::channel(1024);
        for topic in topics {
            let tx = tx.clone();
            let config = self.config.clone();
            tokio::spawn(async move {
                let client = match PubNubClientBuilder::with_reqwest_transport()
                    .with_keyset(Keyset {
                        subscribe_key: config.subscribe_key.as_str(),
                        publish_key: Some(config.publish_key.as_str()),
                        secret_key: None,
                    })
                    .with_user_id(config.user_id.as_str())
                    .build()
                {
                    Ok(client) => client,
                    Err(err) => {
                        let _ = tx.send(Err(DeltaError::Transport(err.to_string()))).await;
                        return;
                    }
                };
                let channel = client.channel(topic.as_str());
                let subscription = channel.subscription(None);
                subscription.subscribe();
                subscription
                    .messages_stream()
                    .for_each(|message| {
                        let tx = tx.clone();
                        let topic = topic.clone();
                        async move {
                            let result = decode_envelope(&message.data)
                                .map(|payload| TransportMessage { topic, payload });
                            let _ = tx.send(result).await;
                        }
                    })
                    .await;
            });
        }
        drop(tx);
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

fn decode_envelope(bytes: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let value: Value = serde_json::from_slice(bytes)?;
    if value.get("protocol").and_then(Value::as_str) != Some("delta-stream/transport/1") {
        return Err(DeltaError::Transport(
            "unsupported PubNub transport envelope".into(),
        ));
    }
    let payload = value
        .get("payload")
        .and_then(Value::as_str)
        .ok_or_else(|| DeltaError::Transport("PubNub envelope missing payload".into()))?;
    hex_decode(payload).map_err(DeltaError::Transport)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex packet has odd length".into());
    }
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        out.push((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?);
    }
    Ok(out)
}
fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err("invalid hex digit".into()),
    }
}
