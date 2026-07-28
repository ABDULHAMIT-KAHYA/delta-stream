use delta_stream::{
    AdaptivePolicy, AgentState, ApplyResult, Decoder, EncodeMode, Encoder, Packet, StatePublisher,
    StateSubscriber,
};
use serde::{Deserialize, Serialize};

#[test]
fn v20_recovery_snapshot_does_not_advance_shared_sequence() {
    let mut encoder = Encoder::default();
    let state = AgentState::demo();
    let _ = encoder.encode(&state).unwrap();
    let before = encoder.sequence();
    let recovery = encoder.recovery_snapshot(&state).unwrap();
    assert_eq!(encoder.sequence(), before);
    assert_eq!(recovery.sequence, before);
}

#[test]
fn v20_four_way_policy_can_choose_compressed_snapshot() {
    let policy = AdaptivePolicy::default();
    let packet = Packet::snapshot(1, 1, 1, vec![0; 128 * 1024]);
    let (_, decision) = policy.select_initial(packet).unwrap();
    assert_eq!(decision.mode, EncodeMode::SnapshotZstd);
}

#[test]
fn v20_accepts_v2_wire_envelope() {
    let mut encoder = Encoder::default();
    let packet = encoder.encode(&AgentState::demo()).unwrap();
    let mut wire = packet.encode().unwrap();
    wire[2] = delta_stream::packet::MIN_WIRE_VERSION;
    assert!(Packet::decode(&wire).is_ok());
}

#[test]
fn v20_stale_reordered_packet_does_not_trigger_rollback() {
    let a = AgentState::demo();
    let b = a.advance();
    let c = b.advance();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();
    let p1 = encoder.encode(&a).unwrap();
    let p2 = encoder.encode(&b).unwrap();
    let p3 = encoder.encode(&c).unwrap();
    let _ = decoder.apply_packet(p1).unwrap();
    let _ = decoder.apply_packet(p2.clone()).unwrap();
    let _ = decoder.apply_packet(p3).unwrap();
    assert!(matches!(
        decoder.apply_packet(p2).unwrap(),
        ApplyResult::Duplicate { .. }
    ));
    assert_eq!(decoder.state(), Some(&c));
}

#[test]
fn v20_multiclient_converges() {
    let report = delta_stream::multi_client::run_deterministic(32, 2_000).unwrap();
    assert!(report.drops > 0);
    assert!(report.reorders > 0);
    assert!(report.disconnects > 0);
    assert!(report.all_converged());
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, delta_stream::DeltaState)]
struct DemoState {
    x: u64,
    label: String,
}

#[test]
fn v20_high_level_api_roundtrips() {
    let mut publisher = StatePublisher::<DemoState>::default();
    let mut subscriber = StateSubscriber::<DemoState>::default();
    let a = DemoState {
        x: 1,
        label: "hello".into(),
    };
    let b = DemoState {
        x: 2,
        label: "hello".into(),
    };
    let p1 = publisher.update(&a).unwrap();
    let p2 = publisher.update(&b).unwrap();
    let _ = subscriber.apply(p1).unwrap();
    let _ = subscriber.apply(p2).unwrap();
    assert_eq!(subscriber.state(), Some(&b));
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OldSchema {
    value: u64,
}

impl delta_stream::DeltaState for OldSchema {
    const SCHEMA_NAME: &'static str = "demo/schema/v1";
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct NewSchema {
    value: u64,
    label: String,
}

impl delta_stream::DeltaState for NewSchema {
    const SCHEMA_NAME: &'static str = "demo/schema/v2";
}

fn migrate_old_to_new(
    mut value: serde_json::Value,
) -> Result<serde_json::Value, delta_stream::DeltaError> {
    value
        .as_object_mut()
        .ok_or(delta_stream::DeltaError::InvalidState(
            "migration expects object",
        ))?
        .insert("label".into(), serde_json::Value::String("migrated".into()));
    Ok(value)
}

#[test]
fn v20_snapshot_schema_migration_is_explicit() {
    use delta_stream::DeltaState;

    let old = OldSchema { value: 7 };
    let bytes = serde_json::to_vec(&old).unwrap();
    let packet = Packet::snapshot(
        1,
        delta_stream::sync::fnv1a64(&bytes),
        OldSchema::schema_hash(),
        bytes,
    );
    let mut migrations = delta_stream::MigrationRegistry::new();
    migrations.register(
        OldSchema::schema_hash(),
        NewSchema::schema_hash(),
        migrate_old_to_new,
    );
    let mut subscriber = StateSubscriber::<NewSchema>::default();
    let _ = subscriber
        .apply_with_migrations(packet, &migrations)
        .unwrap();
    assert_eq!(
        subscriber.state(),
        Some(&NewSchema {
            value: 7,
            label: "migrated".into()
        })
    );
}
