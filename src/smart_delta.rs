use crate::error::DeltaError;

const TAG_SPARSE: u8 = 1;
const TAG_RANGES: u8 = 2;
const TAG_XOR: u8 = 3;
const TAG_SPLICE: u8 = 4;
const TAG_CHUNKS: u8 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SmartDeltaKind {
    Sparse,
    Ranges,
    Xor,
    Splice,
    Chunks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmartDeltaCandidate {
    pub kind: SmartDeltaKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SmartDeltaPolicy {
    pub chunk_size: usize,
    pub max_range_gap: usize,
}

impl Default for SmartDeltaPolicy {
    fn default() -> Self {
        Self {
            chunk_size: 1024,
            max_range_gap: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveTuner {
    pub samples: u64,
    pub compression_attempts: u64,
    pub compression_wins: u64,
    pub min_zstd_payload: usize,
}

impl Default for AdaptiveTuner {
    fn default() -> Self {
        Self {
            samples: 0,
            compression_attempts: 0,
            compression_wins: 0,
            min_zstd_payload: 256,
        }
    }
}

impl AdaptiveTuner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn should_try_zstd(&self, payload_len: usize) -> bool {
        payload_len >= self.min_zstd_payload.max(64)
    }

    pub fn observe_compression(&mut self, before: usize, after: usize) {
        self.samples = self.samples.saturating_add(1);
        self.compression_attempts = self.compression_attempts.saturating_add(1);
        if after < before {
            self.compression_wins = self.compression_wins.saturating_add(1);
        }

        if self.compression_attempts >= 32 {
            let win_rate = self.compression_wins as f64 / self.compression_attempts as f64;
            if win_rate < 0.10 {
                self.min_zstd_payload =
                    self.min_zstd_payload.saturating_mul(2).clamp(64, 64 * 1024);
            } else if win_rate > 0.60 {
                self.min_zstd_payload = (self.min_zstd_payload / 2).max(64);
            }
            self.compression_attempts = 0;
            self.compression_wins = 0;
        }
    }
}

pub fn encode_candidates(
    previous: &[u8],
    current: &[u8],
    policy: SmartDeltaPolicy,
) -> Result<Vec<SmartDeltaCandidate>, DeltaError> {
    let mut out = Vec::with_capacity(5);

    if previous.len() == current.len() {
        out.push(SmartDeltaCandidate {
            kind: SmartDeltaKind::Sparse,
            payload: encode_sparse(previous, current),
        });
        out.push(SmartDeltaCandidate {
            kind: SmartDeltaKind::Ranges,
            payload: encode_ranges(previous, current, policy.max_range_gap),
        });
        out.push(SmartDeltaCandidate {
            kind: SmartDeltaKind::Xor,
            payload: encode_xor(previous, current),
        });
    }

    out.push(SmartDeltaCandidate {
        kind: SmartDeltaKind::Splice,
        payload: encode_splice(previous, current),
    });
    out.push(SmartDeltaCandidate {
        kind: SmartDeltaKind::Chunks,
        payload: encode_chunks(previous, current, policy.chunk_size.max(1)),
    });
    Ok(out)
}

pub fn encode_candidate(
    kind: SmartDeltaKind,
    previous: &[u8],
    current: &[u8],
    policy: SmartDeltaPolicy,
) -> Result<SmartDeltaCandidate, DeltaError> {
    let payload = match kind {
        SmartDeltaKind::Sparse => {
            if previous.len() != current.len() {
                return Err(DeltaError::InvalidState(
                    "sparse delta requires equal lengths",
                ));
            }
            encode_sparse(previous, current)
        }
        SmartDeltaKind::Ranges => {
            if previous.len() != current.len() {
                return Err(DeltaError::InvalidState(
                    "range delta requires equal lengths",
                ));
            }
            encode_ranges(previous, current, policy.max_range_gap)
        }
        SmartDeltaKind::Xor => {
            if previous.len() != current.len() {
                return Err(DeltaError::InvalidState("xor delta requires equal lengths"));
            }
            encode_xor(previous, current)
        }
        SmartDeltaKind::Splice => encode_splice(previous, current),
        SmartDeltaKind::Chunks => encode_chunks(previous, current, policy.chunk_size.max(1)),
    };
    Ok(SmartDeltaCandidate { kind, payload })
}

pub fn apply(base: &[u8], payload: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let (&tag, rest) = payload
        .split_first()
        .ok_or_else(|| DeltaError::InvalidPacket("empty smart delta".into()))?;
    match tag {
        TAG_SPARSE => apply_sparse(base, rest),
        TAG_RANGES => apply_ranges(base, rest),
        TAG_XOR => apply_xor(base, rest),
        TAG_SPLICE => apply_splice(base, rest),
        TAG_CHUNKS => apply_chunks(base, rest),
        other => Err(DeltaError::InvalidPacket(format!(
            "unknown smart delta kind {other}"
        ))),
    }
}

pub fn kind(payload: &[u8]) -> Result<SmartDeltaKind, DeltaError> {
    match payload.first().copied() {
        Some(TAG_SPARSE) => Ok(SmartDeltaKind::Sparse),
        Some(TAG_RANGES) => Ok(SmartDeltaKind::Ranges),
        Some(TAG_XOR) => Ok(SmartDeltaKind::Xor),
        Some(TAG_SPLICE) => Ok(SmartDeltaKind::Splice),
        Some(TAG_CHUNKS) => Ok(SmartDeltaKind::Chunks),
        Some(other) => Err(DeltaError::InvalidPacket(format!(
            "unknown smart delta kind {other}"
        ))),
        None => Err(DeltaError::InvalidPacket("empty smart delta".into())),
    }
}

fn encode_sparse(previous: &[u8], current: &[u8]) -> Vec<u8> {
    let changes: Vec<(usize, u8)> = previous
        .iter()
        .zip(current)
        .enumerate()
        .filter_map(|(i, (&a, &b))| (a != b).then_some((i, b)))
        .collect();
    let mut out = Vec::with_capacity(2 + changes.len() * 3);
    out.push(TAG_SPARSE);
    put_var_u64(&mut out, current.len() as u64);
    put_var_u64(&mut out, changes.len() as u64);
    let mut previous_index = 0usize;
    for (n, (index, value)) in changes.into_iter().enumerate() {
        let delta = if n == 0 {
            index
        } else {
            index - previous_index
        };
        put_var_u64(&mut out, delta as u64);
        out.push(value);
        previous_index = index;
    }
    out
}

fn apply_sparse(base: &[u8], bytes: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let mut cursor = 0usize;
    let new_len = read_var_u64(bytes, &mut cursor)? as usize;
    if new_len != base.len() {
        return Err(DeltaError::InvalidPacket(
            "sparse delta length mismatch".into(),
        ));
    }
    let count = read_var_u64(bytes, &mut cursor)? as usize;
    let mut out = base.to_vec();
    let mut previous_index = 0usize;
    for n in 0..count {
        let delta = read_var_u64(bytes, &mut cursor)? as usize;
        let index = if n == 0 {
            delta
        } else {
            previous_index
                .checked_add(delta)
                .ok_or_else(|| DeltaError::InvalidPacket("sparse index overflow".into()))?
        };
        let value = *bytes
            .get(cursor)
            .ok_or_else(|| DeltaError::InvalidPacket("truncated sparse value".into()))?;
        cursor += 1;
        *out.get_mut(index)
            .ok_or_else(|| DeltaError::InvalidPacket("sparse index out of range".into()))? = value;
        previous_index = index;
    }
    ensure_consumed(bytes, cursor)?;
    Ok(out)
}

fn encode_ranges(previous: &[u8], current: &[u8], max_gap: usize) -> Vec<u8> {
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut i = 0usize;
    while i < current.len() {
        if previous[i] == current[i] {
            i += 1;
            continue;
        }
        let start = i;
        let mut last_changed = i;
        i += 1;
        while i < current.len() {
            if previous[i] != current[i] {
                last_changed = i;
            } else if i.saturating_sub(last_changed) > max_gap {
                break;
            }
            i += 1;
        }
        ranges.push((start, last_changed + 1));
        i = last_changed + 1;
    }

    let mut out = Vec::new();
    out.push(TAG_RANGES);
    put_var_u64(&mut out, current.len() as u64);
    put_var_u64(&mut out, ranges.len() as u64);
    let mut previous_end = 0usize;
    for (start, end) in ranges {
        put_var_u64(&mut out, start.saturating_sub(previous_end) as u64);
        put_var_u64(&mut out, (end - start) as u64);
        out.extend_from_slice(&current[start..end]);
        previous_end = end;
    }
    out
}

fn apply_ranges(base: &[u8], bytes: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let mut cursor = 0usize;
    let new_len = read_var_u64(bytes, &mut cursor)? as usize;
    if new_len != base.len() {
        return Err(DeltaError::InvalidPacket(
            "range delta length mismatch".into(),
        ));
    }
    let count = read_var_u64(bytes, &mut cursor)? as usize;
    let mut out = base.to_vec();
    let mut previous_end = 0usize;
    for _ in 0..count {
        let gap = read_var_u64(bytes, &mut cursor)? as usize;
        let len = read_var_u64(bytes, &mut cursor)? as usize;
        let start = previous_end
            .checked_add(gap)
            .ok_or_else(|| DeltaError::InvalidPacket("range index overflow".into()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| DeltaError::InvalidPacket("range length overflow".into()))?;
        let src_end = cursor
            .checked_add(len)
            .ok_or_else(|| DeltaError::InvalidPacket("range payload overflow".into()))?;
        let src = bytes
            .get(cursor..src_end)
            .ok_or_else(|| DeltaError::InvalidPacket("truncated range bytes".into()))?;
        let dst = out
            .get_mut(start..end)
            .ok_or_else(|| DeltaError::InvalidPacket("range out of bounds".into()))?;
        dst.copy_from_slice(src);
        cursor = src_end;
        previous_end = end;
    }
    ensure_consumed(bytes, cursor)?;
    Ok(out)
}

fn encode_xor(previous: &[u8], current: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(current.len() + 10);
    out.push(TAG_XOR);
    put_var_u64(&mut out, current.len() as u64);
    out.extend(previous.iter().zip(current).map(|(&a, &b)| a ^ b));
    out
}

fn apply_xor(base: &[u8], bytes: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let mut cursor = 0usize;
    let len = read_var_u64(bytes, &mut cursor)? as usize;
    if len != base.len() || bytes.len().saturating_sub(cursor) != len {
        return Err(DeltaError::InvalidPacket(
            "xor delta length mismatch".into(),
        ));
    }
    Ok(base
        .iter()
        .zip(&bytes[cursor..])
        .map(|(&a, &b)| a ^ b)
        .collect())
}

fn encode_splice(previous: &[u8], current: &[u8]) -> Vec<u8> {
    let max_prefix = previous.len().min(current.len());
    let mut prefix = 0usize;
    while prefix < max_prefix && previous[prefix] == current[prefix] {
        prefix += 1;
    }

    let max_suffix = previous
        .len()
        .saturating_sub(prefix)
        .min(current.len().saturating_sub(prefix));
    let mut suffix = 0usize;
    while suffix < max_suffix
        && previous[previous.len() - 1 - suffix] == current[current.len() - 1 - suffix]
    {
        suffix += 1;
    }

    let middle_end = current.len().saturating_sub(suffix);
    let middle = &current[prefix..middle_end];
    let mut out = Vec::with_capacity(middle.len() + 32);
    out.push(TAG_SPLICE);
    put_var_u64(&mut out, current.len() as u64);
    put_var_u64(&mut out, prefix as u64);
    put_var_u64(&mut out, suffix as u64);
    put_var_u64(&mut out, middle.len() as u64);
    out.extend_from_slice(middle);
    out
}

fn apply_splice(base: &[u8], bytes: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let mut cursor = 0usize;
    let new_len = read_var_u64(bytes, &mut cursor)? as usize;
    let prefix = read_var_u64(bytes, &mut cursor)? as usize;
    let suffix = read_var_u64(bytes, &mut cursor)? as usize;
    let middle_len = read_var_u64(bytes, &mut cursor)? as usize;
    if prefix > base.len() || suffix > base.len().saturating_sub(prefix) {
        return Err(DeltaError::InvalidPacket(
            "splice base bounds invalid".into(),
        ));
    }
    let middle_end = cursor
        .checked_add(middle_len)
        .ok_or_else(|| DeltaError::InvalidPacket("splice overflow".into()))?;
    let middle = bytes
        .get(cursor..middle_end)
        .ok_or_else(|| DeltaError::InvalidPacket("truncated splice payload".into()))?;
    ensure_consumed(bytes, middle_end)?;

    let expected_len = prefix
        .checked_add(middle_len)
        .and_then(|v| v.checked_add(suffix))
        .ok_or_else(|| DeltaError::InvalidPacket("splice length overflow".into()))?;
    if expected_len != new_len {
        return Err(DeltaError::InvalidPacket(
            "splice new length mismatch".into(),
        ));
    }

    let mut out = Vec::with_capacity(new_len);
    out.extend_from_slice(&base[..prefix]);
    out.extend_from_slice(middle);
    out.extend_from_slice(&base[base.len() - suffix..]);
    Ok(out)
}

fn encode_chunks(previous: &[u8], current: &[u8], chunk_size: usize) -> Vec<u8> {
    let chunks = current.len().div_ceil(chunk_size);
    let mut changed = Vec::new();
    for chunk in 0..chunks {
        let start = chunk * chunk_size;
        let end = (start + chunk_size).min(current.len());
        let current_chunk = &current[start..end];
        let previous_chunk = previous.get(start..end);
        if previous_chunk != Some(current_chunk) {
            changed.push((chunk, current_chunk));
        }
    }
    let mut out = Vec::new();
    out.push(TAG_CHUNKS);
    put_var_u64(&mut out, current.len() as u64);
    put_var_u64(&mut out, chunk_size as u64);
    put_var_u64(&mut out, changed.len() as u64);
    let mut previous_chunk_index = 0usize;
    for (n, (chunk, data)) in changed.into_iter().enumerate() {
        let index_delta = if n == 0 {
            chunk
        } else {
            chunk - previous_chunk_index
        };
        put_var_u64(&mut out, index_delta as u64);
        put_var_u64(&mut out, data.len() as u64);
        out.extend_from_slice(data);
        previous_chunk_index = chunk;
    }
    out
}

fn apply_chunks(base: &[u8], bytes: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let mut cursor = 0usize;
    let new_len = read_var_u64(bytes, &mut cursor)? as usize;
    let chunk_size = read_var_u64(bytes, &mut cursor)? as usize;
    if chunk_size == 0 {
        return Err(DeltaError::InvalidPacket("zero chunk size".into()));
    }
    let count = read_var_u64(bytes, &mut cursor)? as usize;
    let mut out = base.to_vec();
    out.resize(new_len, 0);
    let mut previous_chunk = 0usize;
    for n in 0..count {
        let delta = read_var_u64(bytes, &mut cursor)? as usize;
        let chunk = if n == 0 {
            delta
        } else {
            previous_chunk
                .checked_add(delta)
                .ok_or_else(|| DeltaError::InvalidPacket("chunk index overflow".into()))?
        };
        let len = read_var_u64(bytes, &mut cursor)? as usize;
        let start = chunk
            .checked_mul(chunk_size)
            .ok_or_else(|| DeltaError::InvalidPacket("chunk offset overflow".into()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| DeltaError::InvalidPacket("chunk length overflow".into()))?;
        let src_end = cursor
            .checked_add(len)
            .ok_or_else(|| DeltaError::InvalidPacket("chunk payload overflow".into()))?;
        let src = bytes
            .get(cursor..src_end)
            .ok_or_else(|| DeltaError::InvalidPacket("truncated chunk payload".into()))?;
        let dst = out
            .get_mut(start..end)
            .ok_or_else(|| DeltaError::InvalidPacket("chunk out of bounds".into()))?;
        dst.copy_from_slice(src);
        cursor = src_end;
        previous_chunk = chunk;
    }
    ensure_consumed(bytes, cursor)?;
    Ok(out)
}

fn ensure_consumed(bytes: &[u8], cursor: usize) -> Result<(), DeltaError> {
    if cursor == bytes.len() {
        Ok(())
    } else {
        Err(DeltaError::InvalidPacket(
            "trailing smart delta bytes".into(),
        ))
    }
}

pub(crate) fn put_var_u64(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn read_var_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, DeltaError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| DeltaError::InvalidPacket("truncated varint".into()))?;
        *cursor += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(DeltaError::InvalidPacket("varint overflow".into()));
        }
    }
}
