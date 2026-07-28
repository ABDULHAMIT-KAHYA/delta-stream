#[cfg(feature = "async-runtime")]
use async_trait::async_trait;
#[cfg(feature = "async-runtime")]
use futures::stream::BoxStream;

#[cfg(feature = "async-runtime")]
use crate::error::DeltaError;

#[cfg(feature = "async-runtime")]
#[derive(Debug, Clone)]
pub struct TransportMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

#[cfg(feature = "async-runtime")]
#[async_trait]
pub trait RealtimeTransport: Clone + Send + Sync + 'static {
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), DeltaError>;
    async fn subscribe(
        &self,
        topics: Vec<String>,
    ) -> Result<BoxStream<'static, Result<TransportMessage, DeltaError>>, DeltaError>;
}

#[cfg(feature = "async-runtime")]
pub mod local;
#[cfg(feature = "mqtt-transport")]
pub mod mqtt;
#[cfg(feature = "nats-transport")]
pub mod nats;
#[cfg(feature = "pubnub-transport")]
pub mod pubnub;
#[cfg(feature = "websocket-transport")]
pub mod websocket;
