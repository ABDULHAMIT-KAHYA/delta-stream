use crate::{error::DeltaError, state::AgentState};

pub const FIELD_AGENT_ID: u16 = 1 << 0;
pub const FIELD_MODEL: u16 = 1 << 1;
pub const FIELD_STATUS: u16 = 1 << 2;
pub const FIELD_TASK: u16 = 1 << 3;
pub const FIELD_PROGRESS: u16 = 1 << 4;
pub const FIELD_TOKENS: u16 = 1 << 5;
pub const FIELD_MEMORY_MB: u16 = 1 << 6;
pub const FIELD_CPU_PERCENT: u16 = 1 << 7;
pub const FIELD_FILES_PROCESSED: u16 = 1 << 8;
pub const FIELD_CURRENT_FILE: u16 = 1 << 9;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AgentDelta {
    pub mask: u16,
    pub agent_id: Option<String>,
    pub model: Option<String>,
    pub status: Option<String>,
    pub task: Option<String>,
    pub progress_delta: Option<i128>,
    pub tokens_delta: Option<i128>,
    pub memory_mb_delta: Option<i128>,
    pub cpu_percent: Option<f32>,
    pub files_processed_delta: Option<i128>,
    pub current_file: Option<String>,
}

impl AgentDelta {
    pub fn between(old: &AgentState, new: &AgentState) -> Self {
        let mut d = Self::default();
        macro_rules! changed_string {
            ($field:ident, $bit:expr) => {
                if old.$field != new.$field {
                    d.mask |= $bit;
                    d.$field = Some(new.$field.clone());
                }
            };
        }
        changed_string!(agent_id, FIELD_AGENT_ID);
        changed_string!(model, FIELD_MODEL);
        changed_string!(status, FIELD_STATUS);
        changed_string!(task, FIELD_TASK);
        if old.progress != new.progress {
            d.mask |= FIELD_PROGRESS;
            d.progress_delta = Some(new.progress as i128 - old.progress as i128);
        }
        if old.tokens != new.tokens {
            d.mask |= FIELD_TOKENS;
            d.tokens_delta = Some(new.tokens as i128 - old.tokens as i128);
        }
        if old.memory_mb != new.memory_mb {
            d.mask |= FIELD_MEMORY_MB;
            d.memory_mb_delta = Some(new.memory_mb as i128 - old.memory_mb as i128);
        }
        if old.cpu_percent != new.cpu_percent {
            d.mask |= FIELD_CPU_PERCENT;
            d.cpu_percent = Some(new.cpu_percent);
        }
        if old.files_processed != new.files_processed {
            d.mask |= FIELD_FILES_PROCESSED;
            d.files_processed_delta =
                Some(new.files_processed as i128 - old.files_processed as i128);
        }
        changed_string!(current_file, FIELD_CURRENT_FILE);
        d
    }

    pub fn apply(&self, base: &AgentState) -> Result<AgentState, DeltaError> {
        let mut out = base.clone();
        macro_rules! apply_string {
            ($field:ident) => {
                if let Some(value) = &self.$field {
                    out.$field = value.clone();
                }
            };
        }
        apply_string!(agent_id);
        apply_string!(model);
        apply_string!(status);
        apply_string!(task);
        if let Some(v) = self.progress_delta {
            out.progress = apply_u8_delta(out.progress, v)?;
        }
        if let Some(v) = self.tokens_delta {
            out.tokens = apply_u64_delta(out.tokens, v)?;
        }
        if let Some(v) = self.memory_mb_delta {
            out.memory_mb = apply_u64_delta(out.memory_mb, v)?;
        }
        if let Some(v) = self.cpu_percent {
            out.cpu_percent = v;
        }
        if let Some(v) = self.files_processed_delta {
            out.files_processed = apply_u64_delta(out.files_processed, v)?;
        }
        apply_string!(current_file);
        Ok(out)
    }
}

pub fn encode_agent_state(state: &AgentState) -> Result<Vec<u8>, DeltaError> {
    let mut out = Vec::with_capacity(96);
    put_string(&mut out, &state.agent_id);
    put_string(&mut out, &state.model);
    put_string(&mut out, &state.status);
    put_string(&mut out, &state.task);
    put_var_u128(&mut out, state.progress as u128);
    put_var_u128(&mut out, state.tokens as u128);
    put_var_u128(&mut out, state.memory_mb as u128);
    out.extend_from_slice(&state.cpu_percent.to_le_bytes());
    put_var_u128(&mut out, state.files_processed as u128);
    put_string(&mut out, &state.current_file);
    Ok(out)
}

pub fn decode_agent_state(bytes: &[u8]) -> Result<AgentState, DeltaError> {
    let mut r = Reader::new(bytes);
    let state = AgentState {
        agent_id: r.string()?,
        model: r.string()?,
        status: r.string()?,
        task: r.string()?,
        progress: u8::try_from(r.var_u128()?)
            .map_err(|_| DeltaError::InvalidPacket("progress overflow".into()))?,
        tokens: u64::try_from(r.var_u128()?)
            .map_err(|_| DeltaError::InvalidPacket("tokens overflow".into()))?,
        memory_mb: u64::try_from(r.var_u128()?)
            .map_err(|_| DeltaError::InvalidPacket("memory overflow".into()))?,
        cpu_percent: r.f32()?,
        files_processed: u64::try_from(r.var_u128()?)
            .map_err(|_| DeltaError::InvalidPacket("files overflow".into()))?,
        current_file: r.string()?,
    };
    r.finish()?;
    Ok(state)
}

pub fn encode_agent_delta(delta: &AgentDelta) -> Result<Vec<u8>, DeltaError> {
    let mut out = Vec::with_capacity(32);
    out.extend_from_slice(&delta.mask.to_le_bytes());
    macro_rules! string_field {
        ($bit:expr, $field:ident) => {
            if delta.mask & $bit != 0 {
                put_string(
                    &mut out,
                    delta
                        .$field
                        .as_deref()
                        .ok_or(DeltaError::InvalidState("delta mask/value mismatch"))?,
                );
            }
        };
    }
    string_field!(FIELD_AGENT_ID, agent_id);
    string_field!(FIELD_MODEL, model);
    string_field!(FIELD_STATUS, status);
    string_field!(FIELD_TASK, task);
    if delta.mask & FIELD_PROGRESS != 0 {
        put_var_i128(
            &mut out,
            delta
                .progress_delta
                .ok_or(DeltaError::InvalidState("delta mask/value mismatch"))?,
        );
    }
    if delta.mask & FIELD_TOKENS != 0 {
        put_var_i128(
            &mut out,
            delta
                .tokens_delta
                .ok_or(DeltaError::InvalidState("delta mask/value mismatch"))?,
        );
    }
    if delta.mask & FIELD_MEMORY_MB != 0 {
        put_var_i128(
            &mut out,
            delta
                .memory_mb_delta
                .ok_or(DeltaError::InvalidState("delta mask/value mismatch"))?,
        );
    }
    if delta.mask & FIELD_CPU_PERCENT != 0 {
        out.extend_from_slice(
            &delta
                .cpu_percent
                .ok_or(DeltaError::InvalidState("delta mask/value mismatch"))?
                .to_le_bytes(),
        );
    }
    if delta.mask & FIELD_FILES_PROCESSED != 0 {
        put_var_i128(
            &mut out,
            delta
                .files_processed_delta
                .ok_or(DeltaError::InvalidState("delta mask/value mismatch"))?,
        );
    }
    string_field!(FIELD_CURRENT_FILE, current_file);
    Ok(out)
}

pub fn decode_agent_delta(bytes: &[u8]) -> Result<AgentDelta, DeltaError> {
    let mut r = Reader::new(bytes);
    let mask = r.u16()?;
    let mut d = AgentDelta {
        mask,
        ..Default::default()
    };
    macro_rules! string_field {
        ($bit:expr, $field:ident) => {
            if mask & $bit != 0 {
                d.$field = Some(r.string()?);
            }
        };
    }
    string_field!(FIELD_AGENT_ID, agent_id);
    string_field!(FIELD_MODEL, model);
    string_field!(FIELD_STATUS, status);
    string_field!(FIELD_TASK, task);
    if mask & FIELD_PROGRESS != 0 {
        d.progress_delta = Some(r.var_i128()?);
    }
    if mask & FIELD_TOKENS != 0 {
        d.tokens_delta = Some(r.var_i128()?);
    }
    if mask & FIELD_MEMORY_MB != 0 {
        d.memory_mb_delta = Some(r.var_i128()?);
    }
    if mask & FIELD_CPU_PERCENT != 0 {
        d.cpu_percent = Some(r.f32()?);
    }
    if mask & FIELD_FILES_PROCESSED != 0 {
        d.files_processed_delta = Some(r.var_i128()?);
    }
    string_field!(FIELD_CURRENT_FILE, current_file);
    r.finish()?;
    Ok(d)
}

fn apply_u64_delta(base: u64, delta: i128) -> Result<u64, DeltaError> {
    let value = base as i128 + delta;
    u64::try_from(value).map_err(|_| DeltaError::InvalidPacket("u64 delta overflow".into()))
}
fn apply_u8_delta(base: u8, delta: i128) -> Result<u8, DeltaError> {
    let value = base as i128 + delta;
    u8::try_from(value).map_err(|_| DeltaError::InvalidPacket("u8 delta overflow".into()))
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_var_u128(out, value.len() as u128);
    out.extend_from_slice(value.as_bytes());
}
fn put_var_i128(out: &mut Vec<u8>, value: i128) {
    let zigzag = ((value << 1) ^ (value >> 127)) as u128;
    put_var_u128(out, zigzag);
}
fn put_var_u128(out: &mut Vec<u8>, mut value: u128) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], DeltaError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| DeltaError::InvalidPacket("length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(DeltaError::InvalidPacket("truncated payload".into()));
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> Result<u8, DeltaError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, DeltaError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("fixed slice"),
        ))
    }
    fn f32(&mut self) -> Result<f32, DeltaError> {
        Ok(f32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed slice"),
        ))
    }
    fn var_u128(&mut self) -> Result<u128, DeltaError> {
        let mut value = 0u128;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            if shift >= 128 && byte & 0x7f != 0 {
                return Err(DeltaError::InvalidPacket("varint overflow".into()));
            }
            value |= ((byte & 0x7f) as u128) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift > 127 {
                return Err(DeltaError::InvalidPacket("varint overflow".into()));
            }
        }
    }
    fn var_i128(&mut self) -> Result<i128, DeltaError> {
        let value = self.var_u128()?;
        Ok(((value >> 1) as i128) ^ (-((value & 1) as i128)))
    }
    fn string(&mut self) -> Result<String, DeltaError> {
        let len = usize::try_from(self.var_u128()?)
            .map_err(|_| DeltaError::InvalidPacket("string length overflow".into()))?;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| DeltaError::InvalidPacket("invalid UTF-8".into()))
    }
    fn finish(&self) -> Result<(), DeltaError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(DeltaError::InvalidPacket("trailing payload bytes".into()))
        }
    }
}
