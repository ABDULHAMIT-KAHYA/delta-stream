use std::marker::PhantomData;

use crate::{
    adaptive::{AdaptivePolicy, EncodeDecision},
    error::DeltaError,
    migration::MigrationRegistry,
    packet::{DecodeConfig, Packet},
    schema::DeltaState,
    sync::{GenericApplyResult, GenericDecoder, GenericEncoder},
};

/// Publishes application-state updates as DeltaStream packets or bytes.
///
/// A publisher owns the sending side of one state stream. It observes each new
/// application state and emits either a snapshot or a delta packet according to
/// the configured adaptive policy. The usual flow is:
///
/// `application state -> Publisher -> snapshot or delta packet -> serialized bytes`.
///
/// Use [`Publisher::encode`] for the simple byte-oriented API, or [`Publisher::update`]
/// when an integration needs direct access to packet metadata before serialization.
///
/// # Example
///
/// ```
/// use delta_stream::{DeltaState, Publisher};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, DeltaState)]
/// struct State {
///     progress: u8,
/// }
///
/// # fn main() -> Result<(), delta_stream::DeltaError> {
/// let mut publisher = Publisher::<State>::new();
/// let bytes = publisher.encode(&State { progress: 10 })?;
/// assert!(!bytes.is_empty());
/// # Ok(())
/// # }
/// ```
pub struct StatePublisher<T: DeltaState> {
    encoder: GenericEncoder<T>,
}

impl<T: DeltaState> Default for StatePublisher<T> {
    fn default() -> Self {
        Self {
            encoder: GenericEncoder::default(),
        }
    }
}

impl<T: DeltaState> StatePublisher<T> {
    /// Creates a publisher with the default adaptive policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts building a publisher with custom settings.
    pub fn builder() -> PublisherBuilder<T> {
        PublisherBuilder::default()
    }

    /// Creates a publisher with an explicit adaptive encoding policy.
    pub fn with_policy(policy: AdaptivePolicy) -> Self {
        Self {
            encoder: GenericEncoder::with_policy(policy),
        }
    }

    /// Encodes a new authoritative state into transport-independent bytes.
    ///
    /// This is the high-level publishing path. It creates either a snapshot or delta
    /// with the same logic as [`Publisher::update`], then serializes that packet with
    /// [`Packet::to_bytes`]. The returned bytes can be carried by any byte-capable
    /// transport, including TCP, WebSocket, PubNub, MQTT, NATS, UDP with reliability
    /// handled externally, files, IPC, or an in-memory channel.
    pub fn encode(&mut self, state: &T) -> Result<Vec<u8>, DeltaError> {
        self.update(state)?.to_bytes()
    }

    /// Encodes a new authoritative state as a packet.
    ///
    /// The first update is a snapshot. Later updates may be deltas or snapshots, and
    /// raw or zstd-compressed, depending on the active [`AdaptivePolicy`]. This method
    /// preserves the stream sequence, schema hash, base hash, and compression behavior
    /// used by the lower-level packet API.
    pub fn update(&mut self, state: &T) -> Result<Packet, DeltaError> {
        self.encoder.encode(state)
    }

    /// Forces a new snapshot and advances the stream sequence.
    pub fn force_snapshot(&mut self, state: &T) -> Result<Packet, DeltaError> {
        self.encoder.force_snapshot(state)
    }

    /// Builds a recovery snapshot at the current stream sequence.
    ///
    /// This does not advance the publisher sequence, so recovering one client does not
    /// create a sequence gap for healthy clients.
    pub fn recovery_snapshot(&self, state: &T) -> Result<Packet, DeltaError> {
        self.encoder.recovery_snapshot(state)
    }

    /// Returns the current publisher sequence.
    pub fn sequence(&self) -> u64 {
        self.encoder.sequence()
    }

    /// Returns the most recent adaptive encoding decision, if one exists.
    pub fn last_decision(&self) -> Option<EncodeDecision> {
        self.encoder.last_decision()
    }
}

/// Builder for [`StatePublisher`].
pub struct PublisherBuilder<T: DeltaState> {
    policy: AdaptivePolicy,
    _marker: PhantomData<T>,
}

impl<T: DeltaState> Default for PublisherBuilder<T> {
    fn default() -> Self {
        Self {
            policy: AdaptivePolicy::default(),
            _marker: PhantomData,
        }
    }
}

impl<T: DeltaState> PublisherBuilder<T> {
    /// Replaces the adaptive encoding policy.
    pub fn adaptive_policy(mut self, policy: AdaptivePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Enables or disables zstd candidate selection.
    pub fn compression(mut self, enabled: bool) -> Self {
        self.policy.enable_zstd = enabled;
        self
    }

    /// Sets the zstd compression level used for compressed candidates.
    pub fn zstd_level(mut self, level: i32) -> Self {
        self.policy.zstd_level = level;
        self
    }

    /// Builds the publisher.
    pub fn build(self) -> StatePublisher<T> {
        StatePublisher::with_policy(self.policy)
    }
}

/// Receives and applies DeltaStream packets for one application-state stream.
///
/// A subscriber owns the receiving side of a stream. The usual flow is:
///
/// `serialized bytes -> Subscriber -> validated synchronized state`.
///
/// It validates schema identity, sequence/base-state relationships, duplicate and stale
/// packets, packet integrity, and decompression limits before committing a new state.
pub struct StateSubscriber<T: DeltaState> {
    decoder: GenericDecoder<T>,
    decode_config: DecodeConfig,
}

impl<T: DeltaState> Default for StateSubscriber<T> {
    fn default() -> Self {
        let decode_config = DecodeConfig::default();
        Self {
            decoder: GenericDecoder::with_decode_config(decode_config),
            decode_config,
        }
    }
}

impl<T: DeltaState> StateSubscriber<T> {
    /// Creates an empty subscriber.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty subscriber with explicit packet decoding limits.
    pub fn with_decode_config(decode_config: DecodeConfig) -> Self {
        Self {
            decoder: GenericDecoder::with_decode_config(decode_config),
            decode_config,
        }
    }

    /// Receives transport bytes, decodes a packet, and applies it.
    ///
    /// This is the high-level receiving path. It parses bytes with
    /// [`Packet::from_bytes_with_config`], validates packet integrity, decompresses within
    /// configured limits when needed, and then applies the packet with the same state-chain
    /// logic as [`Subscriber::apply`]. It may apply a new state, report a duplicate, or
    /// request a snapshot when a gap or incompatible base is detected.
    pub fn receive(&mut self, bytes: &[u8]) -> Result<GenericApplyResult<T>, DeltaError> {
        let packet = Packet::from_bytes_with_config(bytes, &self.decode_config)?;
        self.apply(packet)
    }

    /// Applies a snapshot or delta packet.
    ///
    /// Snapshots establish or restore state. Deltas are committed only when the local
    /// sequence and base-state hash match the packet metadata. Rejected packets do not
    /// mutate the subscriber state.
    pub fn apply(&mut self, packet: Packet) -> Result<GenericApplyResult<T>, DeltaError> {
        self.decoder.apply_packet(packet)
    }

    /// Applies a packet and allows registered snapshot migrations when schemas differ.
    pub fn apply_with_migrations(
        &mut self,
        packet: Packet,
        migrations: &MigrationRegistry,
    ) -> Result<GenericApplyResult<T>, DeltaError> {
        self.decoder
            .apply_packet_with_migrations(packet, migrations)
    }

    /// Returns the last synchronized state.
    pub fn state(&self) -> Option<&T> {
        self.decoder.state()
    }

    /// Returns the last applied sequence.
    pub fn sequence(&self) -> Option<u64> {
        self.decoder.sequence()
    }

    /// Clears local state and sequence tracking.
    pub fn reset(&mut self) {
        self.decoder = GenericDecoder::with_decode_config(self.decode_config);
    }
}

/// Short public name for [`StatePublisher`].
pub type Publisher<T> = StatePublisher<T>;

/// Short public name for [`StateSubscriber`].
pub type Subscriber<T> = StateSubscriber<T>;
