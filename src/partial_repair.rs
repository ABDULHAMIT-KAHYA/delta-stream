use crate::{error::DeltaError, sync::fnv1a64};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkManifest {
    pub state_len: usize,
    pub chunk_size: usize,
    pub hashes: Vec<u64>,
}

impl ChunkManifest {
    pub fn build(state: &[u8], chunk_size: usize) -> Self {
        let chunk_size = chunk_size.max(1);
        let hashes = state.chunks(chunk_size).map(fnv1a64).collect();
        Self {
            state_len: state.len(),
            chunk_size,
            hashes,
        }
    }

    pub fn differing_chunks(&self, other: &Self) -> Vec<usize> {
        let max = self.hashes.len().max(other.hashes.len());
        (0..max)
            .filter(|&i| self.hashes.get(i) != other.hashes.get(i))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPatch {
    pub index: usize,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialRepair {
    pub target_len: usize,
    pub chunk_size: usize,
    pub target_hash: u64,
    pub patches: Vec<ChunkPatch>,
}

impl PartialRepair {
    pub fn build(receiver_state: &[u8], authoritative: &[u8], chunk_size: usize) -> Self {
        let chunk_size = chunk_size.max(1);
        let receiver = ChunkManifest::build(receiver_state, chunk_size);
        let target = ChunkManifest::build(authoritative, chunk_size);
        let patches = receiver
            .differing_chunks(&target)
            .into_iter()
            .filter_map(|index| {
                let start = index.checked_mul(chunk_size)?;
                if start >= authoritative.len() {
                    return Some(ChunkPatch {
                        index,
                        bytes: Vec::new(),
                    });
                }
                let end = (start + chunk_size).min(authoritative.len());
                Some(ChunkPatch {
                    index,
                    bytes: authoritative[start..end].to_vec(),
                })
            })
            .collect();
        Self {
            target_len: authoritative.len(),
            chunk_size,
            target_hash: fnv1a64(authoritative),
            patches,
        }
    }

    pub fn apply(&self, base: &[u8]) -> Result<Vec<u8>, DeltaError> {
        let mut out = base.to_vec();
        out.resize(self.target_len, 0);
        for patch in &self.patches {
            let start = patch
                .index
                .checked_mul(self.chunk_size)
                .ok_or(DeltaError::InvalidState("chunk index overflow"))?;
            if patch.bytes.is_empty() && start >= out.len() {
                continue;
            }
            let end = start
                .checked_add(patch.bytes.len())
                .ok_or(DeltaError::InvalidState("chunk length overflow"))?;
            if end > out.len() {
                return Err(DeltaError::InvalidPacket(
                    "partial repair chunk out of bounds".into(),
                ));
            }
            out[start..end].copy_from_slice(&patch.bytes);
        }
        if fnv1a64(&out) != self.target_hash {
            return Err(DeltaError::InvalidState("partial repair hash mismatch"));
        }
        Ok(out)
    }

    pub fn payload_bytes(&self) -> usize {
        self.patches.iter().map(|p| p.bytes.len()).sum()
    }
}
