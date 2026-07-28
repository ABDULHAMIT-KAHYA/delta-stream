use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    error::DeltaError,
    transport::{RealtimeTransport, TransportMessage},
};

#[derive(Debug, Clone)]
pub struct NatsRealtimeTransport {
    server: String,
}
impl NatsRealtimeTransport {
    pub fn new(server: impl Into<String>) -> Self {
        Self {
            server: server.into(),
        }
    }
    pub fn from_env() -> Self {
        Self::new(std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()))
    }
}

#[async_trait]
impl RealtimeTransport for NatsRealtimeTransport {
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), DeltaError> {
        let client = async_nats::connect(&self.server)
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        client
            .publish(topic.to_string(), payload.into())
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        client
            .flush()
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
            let server = self.server.clone();
            tokio::spawn(async move {
                let client = match async_nats::connect(server).await {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx.send(Err(DeltaError::Transport(e.to_string()))).await;
                        return;
                    }
                };
                let mut sub = match client.subscribe(topic.clone()).await {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx.send(Err(DeltaError::Transport(e.to_string()))).await;
                        return;
                    }
                };
                while let Some(message) = sub.next().await {
                    if tx
                        .send(Ok(TransportMessage {
                            topic: topic.clone(),
                            payload: message.payload.to_vec(),
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        drop(tx);
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
