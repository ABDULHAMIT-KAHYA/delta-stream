use std::marker::PhantomData;

use crate::{
    adaptive::{AdaptivePolicy, EncodeDecision},
    error::DeltaError,
    migration::MigrationRegistry,
    packet::Packet,
    schema::DeltaState,
    sync::{GenericApplyResult, GenericDecoder, GenericEncoder},
};

/// Publishes application-state updates as DeltaStream packets.
///
/// The first update is encoded as a snapshot. Later updates may be encoded as
/// deltas or compressed packets according to the active [`AdaptivePolicy`].
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
/// let packet = publisher.update(&State { progress: 10 })?;
/// assert_eq!(packet.sequence, 1);
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

    /// Encodes a new authoritative state.
    pub fn update(&mut self, state: &T) -> Result<Packet, DeltaError> {
        self.encoder.encode(state)
    }

    /// Forces a new snapshot and advances the stream sequence.
    pub fn force_snapshot(&mut self, state: &T) -> Result<Packet, DeltaError> {
        self.encoder.force_snapshot(state)
    }

    /// Builds a recovery snapshot at the current stream sequence.
    ///
    /// This does not advance the publisher sequence, so recovering one client
    /// does not create a sequence gap for healthy clients.
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
pub struct StateSubscriber<T: DeltaState> {
    decoder: GenericDecoder<T>,
}

impl<T: DeltaState> Default for StateSubscriber<T> {
    fn default() -> Self {
        Self {
            decoder: GenericDecoder::default(),
        }
    }
}

impl<T: DeltaState> StateSubscriber<T> {
    /// Creates an empty subscriber.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a snapshot or delta packet.
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
        self.decoder.reset();
    }
}

/// Short public name for [`StatePublisher`].
pub type Publisher<T> = StatePublisher<T>;

/// Short public name for [`StateSubscriber`].
pub type Subscriber<T> = StateSubscriber<T>;
