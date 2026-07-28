use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Mutex;

use crate::{
    control::ControlMessage,
    error::DeltaError,
    packet::{Packet, PacketKind},
    recovery_history::{RecoveryHistory, RecoveryPlan},
    state::AgentState,
    sync::{ApplyResult, Decoder, Encoder},
    transport::RealtimeTransport,
};

#[derive(Debug, Clone)]
pub struct SessionTopics {
    pub data: String,
    pub control: String,
}
impl SessionTopics {
    pub fn new(base: impl Into<String>) -> Self {
        let data = base.into();
        Self {
            control: format!("{data}.control"),
            data,
        }
    }
    pub fn reply_for(&self, client_id: &str) -> String {
        format!("{}.reply.{client_id}", self.data)
    }
}

#[derive(Clone)]
pub struct PublisherSession<T: RealtimeTransport> {
    transport: T,
    topics: SessionTopics,
    encoder: Arc<Mutex<Encoder>>,
    state: Arc<Mutex<AgentState>>,
    history: Arc<Mutex<RecoveryHistory>>,
}

impl<T: RealtimeTransport> PublisherSession<T> {
    pub fn new(transport: T, topics: SessionTopics, initial: AgentState) -> Self {
        Self {
            transport,
            topics,
            encoder: Arc::new(Mutex::new(Encoder::default())),
            state: Arc::new(Mutex::new(initial)),
            history: Arc::new(Mutex::new(RecoveryHistory::default())),
        }
    }

    pub async fn publish_state(&self, state: AgentState) -> Result<Packet, DeltaError> {
        *self.state.lock().await = state.clone();
        let mut encoder = self.encoder.lock().await;
        let packet = encoder.encode(&state)?;
        let bytes = packet.encode()?;
        self.transport.publish(&self.topics.data, bytes).await?;
        self.history.lock().await.record(packet.clone());
        Ok(packet)
    }

    pub async fn control_loop(&self) -> Result<(), DeltaError> {
        let mut stream = self
            .transport
            .subscribe(vec![self.topics.control.clone()])
            .await?;
        while let Some(message) = stream.next().await {
            let message = message?;
            let control: ControlMessage = match serde_json::from_slice(&message.payload) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("ignored control message: {err}");
                    continue;
                }
            };
            match control {
                ControlMessage::ResyncRequest {
                    client_id,
                    local_sequence,
                    required_sequence,
                    reply_channel,
                } => {
                    println!("RESYNC_REQUEST from {client_id}: local={local_sequence:?} required={required_sequence}");
                    let state = self.state.lock().await.clone();
                    let encoder = self.encoder.lock().await;
                    let snapshot = encoder.recovery_snapshot(&state)?;
                    drop(encoder);
                    let plan = self.history.lock().await.plan(local_sequence, snapshot)?;
                    match plan {
                        RecoveryPlan::Replay(packets) => {
                            let first = packets.first().map(|p| p.sequence).unwrap_or(0);
                            let last = packets.last().map(|p| p.sequence).unwrap_or(0);
                            let count = packets.len();
                            for packet in packets {
                                self.transport
                                    .publish(&reply_channel, packet.encode()?)
                                    .await?;
                            }
                            println!("published recovery REPLAY {first}..={last} ({count} packets) -> {reply_channel}");
                        }
                        RecoveryPlan::Snapshot(snapshot) => {
                            let seq = snapshot.sequence;
                            self.transport
                                .publish(&reply_channel, snapshot.encode()?)
                                .await?;
                            println!("published recovery SNAPSHOT seq={seq} -> {reply_channel}");
                        }
                    }
                }
                ControlMessage::Ack {
                    client_id,
                    sequence,
                } => {
                    println!("ACK client={client_id} seq={sequence}");
                }
                ControlMessage::Hello {
                    client_id,
                    min_version,
                    max_version,
                    reply_channel,
                } => {
                    let local_max = crate::packet::WIRE_VERSION;
                    let selected = local_max;
                    if selected < min_version || selected > max_version {
                        eprintln!("client {client_id} has incompatible protocol range {min_version}..={max_version}");
                        continue;
                    }
                    let ack = ControlMessage::HelloAck {
                        client_id,
                        selected_version: selected,
                    };
                    self.transport
                        .publish(&reply_channel, serde_json::to_vec(&ack)?)
                        .await?;
                }
                ControlMessage::HelloAck { .. } => {}
            }
        }
        Ok(())
    }
}

pub struct SubscriberSession<T: RealtimeTransport> {
    transport: T,
    topics: SessionTopics,
    client_id: String,
    reply_topic: String,
    decoder: Decoder,
    awaiting_snapshot: bool,
}

impl<T: RealtimeTransport> SubscriberSession<T> {
    pub fn new(transport: T, topics: SessionTopics, client_id: impl Into<String>) -> Self {
        let client_id = client_id.into();
        let reply_topic = topics.reply_for(&client_id);
        Self {
            transport,
            topics,
            client_id,
            reply_topic,
            decoder: Decoder::default(),
            awaiting_snapshot: false,
        }
    }

    pub async fn run(mut self, drop_once_sequence: Option<u64>) -> Result<(), DeltaError> {
        let mut stream = self
            .transport
            .subscribe(vec![self.topics.data.clone(), self.reply_topic.clone()])
            .await?;
        let hello = ControlMessage::hello(self.client_id.clone(), self.reply_topic.clone());
        self.transport
            .publish(&self.topics.control, serde_json::to_vec(&hello)?)
            .await?;

        let mut dropped = false;
        while let Some(message) = stream.next().await {
            let message = message?;

            if message.topic == self.reply_topic {
                if let Ok(ControlMessage::HelloAck {
                    client_id,
                    selected_version,
                }) = serde_json::from_slice::<ControlMessage>(&message.payload)
                {
                    println!("protocol negotiated: client={client_id} version={selected_version}");
                    continue;
                }
            }

            let packet = match Packet::decode(&message.payload) {
                Ok(packet) => packet,
                Err(err) => {
                    eprintln!("ignored non-packet message on {}: {err}", message.topic);
                    continue;
                }
            };

            if !dropped
                && drop_once_sequence == Some(packet.sequence)
                && packet.kind == PacketKind::Delta
            {
                dropped = true;
                println!("DEMO DROP: intentionally dropping seq={}", packet.sequence);
                continue;
            }

            let was_awaiting = self.awaiting_snapshot;
            match self.decoder.apply_packet(packet)? {
                ApplyResult::Applied { sequence, state } => {
                    if was_awaiting {
                        self.awaiting_snapshot = false;
                        println!("SYNC RESTORED at seq={sequence}");
                    }
                    println!(
                        "applied seq={sequence} progress={} tokens={} cpu={:.1}%",
                        state.progress, state.tokens, state.cpu_percent
                    );
                    let ack = ControlMessage::Ack {
                        client_id: self.client_id.clone(),
                        sequence,
                    };
                    self.transport
                        .publish(&self.topics.control, serde_json::to_vec(&ack)?)
                        .await?;
                }
                ApplyResult::Duplicate { sequence } => {
                    println!("duplicate seq={sequence} ignored");
                }
                ApplyResult::NeedSnapshot {
                    local_sequence,
                    required_sequence,
                } => {
                    println!("DESYNC detected: local={local_sequence:?} requires base={required_sequence}");
                    if !self.awaiting_snapshot {
                        self.awaiting_snapshot = true;
                        let request = ControlMessage::ResyncRequest {
                            client_id: self.client_id.clone(),
                            local_sequence,
                            required_sequence,
                            reply_channel: self.reply_topic.clone(),
                        };
                        self.transport
                            .publish(&self.topics.control, serde_json::to_vec(&request)?)
                            .await?;
                        println!("automatic RESYNC_REQUEST published");
                    }
                }
            }
        }
        Ok(())
    }
}
