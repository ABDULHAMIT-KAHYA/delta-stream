use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

use crate::{error::DeltaError, sync::fnv1a64};

/// Describes an application state type that DeltaStream can synchronize.
///
/// State types must be cloneable and Serde-serializable because snapshots are encoded as
/// JSON and generic deltas are computed from JSON object fields. Implement this trait
/// manually to choose a stable schema name, or enable the default `derive` feature and use
/// `#[derive(delta_stream::DeltaState)]` on a named struct.
pub trait DeltaState: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    const SCHEMA_NAME: &'static str;
    fn schema_hash() -> u64 {
        fnv1a64(Self::SCHEMA_NAME.as_bytes())
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub struct JsonFieldDelta {
    pub changed: Map<String, Value>,
    pub removed: Vec<String>,
}

impl JsonFieldDelta {
    pub fn between<T: Serialize>(old: &T, new: &T) -> Result<Self, DeltaError> {
        let old = serde_json::to_value(old)?;
        let new = serde_json::to_value(new)?;
        let old = old.as_object().ok_or(DeltaError::InvalidState(
            "generic DeltaState must serialize as a JSON object",
        ))?;
        let new = new.as_object().ok_or(DeltaError::InvalidState(
            "generic DeltaState must serialize as a JSON object",
        ))?;
        let mut changed = Map::new();
        let mut removed = Vec::new();
        for (key, value) in new {
            if old.get(key) != Some(value) {
                changed.insert(key.clone(), value.clone());
            }
        }
        for key in old.keys() {
            if !new.contains_key(key) {
                removed.push(key.clone());
            }
        }
        Ok(Self { changed, removed })
    }

    pub fn apply<T: Serialize + DeserializeOwned>(&self, base: &T) -> Result<T, DeltaError> {
        let mut value = serde_json::to_value(base)?;
        let obj = value.as_object_mut().ok_or(DeltaError::InvalidState(
            "generic DeltaState must serialize as a JSON object",
        ))?;
        for key in &self.removed {
            obj.remove(key);
        }
        for (key, value) in &self.changed {
            obj.insert(key.clone(), value.clone());
        }
        Ok(serde_json::from_value(value)?)
    }
}

pub fn encode_generic_snapshot<T: Serialize>(state: &T) -> Result<Vec<u8>, DeltaError> {
    Ok(serde_json::to_vec(state)?)
}
pub fn decode_generic_snapshot<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DeltaError> {
    Ok(serde_json::from_slice(bytes)?)
}
pub fn encode_generic_delta(delta: &JsonFieldDelta) -> Result<Vec<u8>, DeltaError> {
    Ok(serde_json::to_vec(delta)?)
}
pub fn decode_generic_delta(bytes: &[u8]) -> Result<JsonFieldDelta, DeltaError> {
    Ok(serde_json::from_slice(bytes)?)
}
