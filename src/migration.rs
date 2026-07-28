use std::collections::HashMap;

use serde_json::Value;

use crate::error::DeltaError;

pub type MigrationFn = fn(Value) -> Result<Value, DeltaError>;

/// Explicit one-hop schema migrations for snapshot payloads.
///
/// V20 never applies a delta across schema versions. A mismatched client must
/// receive a snapshot, which can then be migrated through an explicitly
/// registered transformation.
#[derive(Default)]
pub struct MigrationRegistry {
    routes: HashMap<(u64, u64), MigrationFn>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, from_schema: u64, to_schema: u64, migration: MigrationFn) {
        self.routes.insert((from_schema, to_schema), migration);
    }

    pub fn contains(&self, from_schema: u64, to_schema: u64) -> bool {
        self.routes.contains_key(&(from_schema, to_schema))
    }

    pub fn migrate(
        &self,
        from_schema: u64,
        to_schema: u64,
        value: Value,
    ) -> Result<Value, DeltaError> {
        if from_schema == to_schema {
            return Ok(value);
        }
        let migration = self.routes.get(&(from_schema, to_schema)).ok_or(
            DeltaError::SchemaMigrationMissing {
                from: from_schema,
                to: to_schema,
            },
        )?;
        migration(value)
    }
}
