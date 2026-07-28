use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    error::DeltaError,
    transport::{RealtimeTransport, TransportMessage},
};

#[derive(Debug, Clone)]
pub struct LocalTransport {
    tx: Arc<broadcast::Sender<TransportMessage>>,
}

impl Default for LocalTransport {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { tx: Arc::new(tx) }
    }
}

#[async_trait]
impl RealtimeTransport for LocalTransport {
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), DeltaError> {
        let _ = self.tx.send(TransportMessage {
            topic: topic.to_string(),
            payload,
        });
        Ok(())
    }

    async fn subscribe(
        &self,
        topics: Vec<String>,
    ) -> Result<BoxStream<'static, Result<TransportMessage, DeltaError>>, DeltaError> {
        let rx = self.tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            let topics = topics.clone();
            async move {
                match item {
                    Ok(message) if topics.iter().any(|topic| topic == &message.topic) => {
                        Some(Ok(message))
                    }
                    Ok(_) => None,
                    Err(err) => Some(Err(DeltaError::Transport(err.to_string()))),
                }
            }
        });
        Ok(Box::pin(stream))
    }
}
