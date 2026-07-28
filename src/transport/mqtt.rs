use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    error::DeltaError,
    transport::{RealtimeTransport, TransportMessage},
};

#[derive(Debug, Clone)]
pub struct MqttRealtimeTransport {
    host: String,
    port: u16,
    client_prefix: String,
}
impl MqttRealtimeTransport {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            client_prefix: "delta-stream".into(),
        }
    }
    pub fn from_env() -> Self {
        let host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = std::env::var("MQTT_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1883);
        Self::new(host, port)
    }
    fn options(&self, suffix: &str) -> MqttOptions {
        let mut options = MqttOptions::new(
            format!("{}-{suffix}", self.client_prefix),
            self.host.clone(),
            self.port,
        );
        options.set_keep_alive(Duration::from_secs(10));
        options
    }
}

#[async_trait]
impl RealtimeTransport for MqttRealtimeTransport {
    async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), DeltaError> {
        let (client, mut eventloop) = AsyncClient::new(self.options("pub"), 16);
        client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await
            .map_err(|e| DeltaError::Transport(e.to_string()))?;
        for _ in 0..8 {
            match tokio::time::timeout(Duration::from_secs(2), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Incoming::PubAck(_)))) => return Ok(()),
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(DeltaError::Transport(e.to_string())),
                Err(_) => break,
            }
        }
        Ok(())
    }

    async fn subscribe(
        &self,
        topics: Vec<String>,
    ) -> Result<BoxStream<'static, Result<TransportMessage, DeltaError>>, DeltaError> {
        let (client, mut eventloop) = AsyncClient::new(self.options("sub"), 64);
        for topic in &topics {
            client
                .subscribe(topic, QoS::AtLeastOnce)
                .await
                .map_err(|e| DeltaError::Transport(e.to_string()))?;
        }
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        if tx
                            .send(Ok(TransportMessage {
                                topic: p.topic,
                                payload: p.payload.to_vec(),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        let _ = tx.send(Err(DeltaError::Transport(e.to_string()))).await;
                        break;
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
