//! DeltaStream is a state-aware synchronization library for realtime Rust applications.
//!
//! It sends an initial snapshot and then compact state deltas when doing so is cheaper,
//! while validating sequence/base-state relationships on the receiver. When the chain
//! breaks, the receiver reports that recovery is required instead of applying an unsafe
//! delta.
//!
//! # Quick start
//!
//! ```
//! use delta_stream::{DeltaState, Publisher, Subscriber};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
//! struct GameState {
//!     x: i32,
//!     hp: u16,
//! }
//!
//! # fn main() -> Result<(), delta_stream::DeltaError> {
//! let mut publisher = Publisher::<GameState>::new();
//! let mut subscriber = Subscriber::<GameState>::new();
//!
//! let bytes = publisher.encode(&GameState { x: 10, hp: 100 })?;
//! subscriber.receive(&bytes)?;
//!
//! assert_eq!(subscriber.state().map(|state| state.hp), Some(100));
//! # Ok(())
//! # }
//! ```
//!
//! # Synchronization flow
//!
//! ```text
//! Application state
//!     -> Publisher
//!     -> snapshot or delta packet
//!     -> serialized bytes
//!     -> any byte-capable transport
//!     -> received bytes
//!     -> Subscriber
//!     -> validated synchronized state
//! ```
//!
//! # Transport model
//!
//! DeltaStream produces and consumes [`Packet`] values. A transport is responsible for
//! moving the serialized packet bytes between peers. Optional adapters are available for
//! PubNub, WebSocket, MQTT, and NATS behind Cargo features.

extern crate self as delta_stream;

pub mod api;
#[cfg(feature = "crdt")]
pub mod crdt;
pub mod error;
pub mod packet;
pub mod partial_repair;
pub mod recovery_history;
pub mod reorder;
pub mod runtime;
pub mod schema;
#[cfg(feature = "async-runtime")]
pub mod session;
#[cfg(feature = "async-runtime")]
pub mod transport;

// Compatibility and advanced building blocks remain public in v0.30.0 so existing users
// are not broken, but they are hidden from the primary rustdoc surface.
#[doc(hidden)]
pub mod adaptive;
#[doc(hidden)]
pub mod app;
#[doc(hidden)]
pub mod binary;
#[doc(hidden)]
pub mod byte_sync;
#[doc(hidden)]
pub mod chaos;
#[doc(hidden)]
pub mod compat;
#[doc(hidden)]
pub mod control;
#[doc(hidden)]
pub mod demo_server;
#[doc(hidden)]
pub mod edge_cases;
#[doc(hidden)]
pub mod fast_selector;
#[doc(hidden)]
pub mod metrics;
#[doc(hidden)]
pub mod migration;
#[doc(hidden)]
pub mod multi_client;
#[doc(hidden)]
pub mod recovery_v30;
#[doc(hidden)]
pub mod replay;
#[doc(hidden)]
pub mod smart_delta;
#[doc(hidden)]
pub mod state;
#[doc(hidden)]
pub mod sync;
#[doc(hidden)]
pub mod torture;
#[doc(hidden)]
pub mod v25_bench;
#[doc(hidden)]
pub mod v25_validation;
#[doc(hidden)]
pub mod v30_bench;
#[doc(hidden)]
pub mod v30_sync;
#[doc(hidden)]
pub mod v30_torture;
#[doc(hidden)]
pub mod v30_validation;
#[doc(hidden)]
pub mod validation;
#[cfg(feature = "websocket-transport")]
#[doc(hidden)]
pub mod ws_broker;

pub use api::{Publisher, PublisherBuilder, StatePublisher, StateSubscriber, Subscriber};
pub use error::DeltaError;
pub use packet::{
    DecodeConfig, Packet, PacketKind, MAX_LOGICAL_PAYLOAD, MAX_WIRE_PAYLOAD, WIRE_VERSION,
};
pub use partial_repair::{ChunkManifest, ChunkPatch, PartialRepair};
pub use runtime::{backpressure_decision, BackpressureDecision, RuntimeLimits, RuntimeMetrics};
pub use schema::DeltaState;
pub use sync::GenericApplyResult as Apply;

#[cfg(feature = "derive")]
pub use delta_stream_derive::DeltaState;

/// Common imports for application code.
pub mod prelude {
    pub use crate::{Apply, DeltaError, DeltaState, Packet, PacketKind, Publisher, Subscriber};
}

/// Lower-level protocol and tuning types for advanced integrations.
pub mod advanced {
    pub use crate::adaptive::{AdaptivePolicy, EncodeDecision, EncodeMode};
    pub use crate::byte_sync::{
        ByteApplyResult, ByteEncodeDecision, ByteEncodeMode, ByteStateDecoder, ByteStateEncoder,
        V25Policy,
    };
    pub use crate::compat::Capabilities;
    pub use crate::fast_selector::{ChangeProfile, SelectorPolicy, StrategyAdvisor};
    pub use crate::migration::MigrationRegistry;
    pub use crate::recovery_history::{RecoveryHistory, RecoveryPlan};
    pub use crate::recovery_v30::{plan_recovery, V30RecoveryPlan};
    pub use crate::reorder::{ReorderApplyResult, ReorderDecoder};
    pub use crate::smart_delta::{AdaptiveTuner, SmartDeltaKind, SmartDeltaPolicy};
    pub use crate::state::AgentState;
    pub use crate::sync::{
        ApplyResult, Decoder, Encoder, GenericApplyResult, GenericDecoder, GenericEncoder,
    };
    pub use crate::v30_sync::{FastByteStateEncoder, V30Decision, V30EncodeMode, V30Policy};
}

// Backward-compatible root re-exports retained for v0.30.0.
#[doc(hidden)]
pub use adaptive::{AdaptivePolicy, EncodeDecision, EncodeMode};
#[doc(hidden)]
pub use byte_sync::{
    ByteApplyResult, ByteEncodeDecision, ByteEncodeMode, ByteStateDecoder, ByteStateEncoder,
    V25Policy,
};
#[doc(hidden)]
pub use compat::Capabilities;
#[doc(hidden)]
pub use fast_selector::{ChangeProfile, SelectorPolicy, StrategyAdvisor};
#[doc(hidden)]
pub use migration::MigrationRegistry;
#[doc(hidden)]
pub use multi_client::MultiClientReport;
#[doc(hidden)]
pub use recovery_history::{RecoveryHistory, RecoveryPlan};
#[doc(hidden)]
pub use recovery_v30::{plan_recovery, V30RecoveryPlan};
#[doc(hidden)]
pub use smart_delta::{AdaptiveTuner, SmartDeltaKind, SmartDeltaPolicy};
#[doc(hidden)]
pub use state::AgentState;
#[doc(hidden)]
pub use sync::{ApplyResult, Decoder, Encoder, GenericApplyResult, GenericDecoder, GenericEncoder};
#[doc(hidden)]
pub use torture::TortureReport;
#[doc(hidden)]
pub use v30_sync::{FastByteStateEncoder, V30Decision, V30EncodeMode, V30Policy};
#[doc(hidden)]
pub use v30_torture::V30TortureReport;

/// Runs the bundled CLI/demo application.
#[doc(hidden)]
pub use app::run;
