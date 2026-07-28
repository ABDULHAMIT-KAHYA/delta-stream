use serde::{Deserialize, Serialize};

use crate::packet::{MIN_WIRE_VERSION, WIRE_VERSION};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    Hello {
        client_id: String,
        min_version: u8,
        max_version: u8,
        reply_channel: String,
    },
    HelloAck {
        client_id: String,
        selected_version: u8,
    },
    Ack {
        client_id: String,
        sequence: u64,
    },
    ResyncRequest {
        client_id: String,
        local_sequence: Option<u64>,
        required_sequence: u64,
        reply_channel: String,
    },
}

impl ControlMessage {
    pub fn hello(client_id: impl Into<String>, reply_channel: impl Into<String>) -> Self {
        Self::Hello {
            client_id: client_id.into(),
            min_version: MIN_WIRE_VERSION,
            max_version: WIRE_VERSION,
            reply_channel: reply_channel.into(),
        }
    }
}
